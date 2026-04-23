# -*- coding: utf-8 -*-
"""
REMMAQ – Descarga + Extracción (NiFi)
- Fase 1: descarga TODOS los .rar/.zip/.7z a --raw_dir (originales) con .part + rename atómico.
- Fase 2: extrae SOLO .xlsx/.xls/.csv a --extract_dir (extraidos), sin carpetas temporales.
- Para .rar usa rarfile+unrar; si falla o no está, usa 7z/7zz con patrones.
- No re-descarga si ya existe; no re-extrae si el dataset ya está en extraidos.
- Elimina el .rar al terminar la extracción OK.
- Manifiesto CSV: logs/extraccion_manifiesto.csv
- Resumen JSON:   logs/extraccion_resumen.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple, Dict, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# --------------------------- Constantes ---------------------------

HTTP_RETRIES = 3
HTTP_TIMEOUT = 60
DOWNLOAD_RETRIES = 3
EXTRACT_RETRIES = 3

FINAL_EXTS = {".xlsx", ".xls", ".csv"}        # solo estos salen a extraidos
COMP_EXTS  = [".rar", ".zip", ".7z"]          # comprimidos soportados

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "remmaq-etl/nifi"})

# --------------------------- Utilidades ---------------------------

def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()

def _sanitize(name: str) -> str:
    """Nombre de archivo seguro para el filesystem."""
    n = re.sub(r"\s+", "_", name or "")
    n = re.sub(r"[^A-Za-z0-9_.\\-]", "", n)
    return n.strip("_") or "archivo"

def _canon(s: str) -> str:
    """Normaliza cadena para matching laxo de dataset."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())

def _abs(base: str, href: str) -> str:
    """URL absoluta (desde índice)."""
    return href if urlparse(href).scheme else urljoin(base, href)

def _http_get(url: str) -> requests.Response:
    last = None
    for i in range(1, HTTP_RETRIES + 1):
        try:
            r = SESSION.get(url, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r
        except Exception as exc:
            last = exc
            time.sleep(i)
    raise RuntimeError(f"GET falló: {url} -> {last}")

def _soup(url: str) -> BeautifulSoup:
    return BeautifulSoup(_http_get(url).text, "html.parser")

def _looks_archive(s: str) -> bool:
    s = (s or "").lower()
    return any(s.endswith(ext) for ext in COMP_EXTS) or any(k in s for k in ("rar","zip","7z"))

def _find_links(base_url: str) -> List[Tuple[str, str]]:
    soup = _soup(base_url)
    out = []
    for a in soup.find_all("a"):
        href = a.get("href")
        if href:
            out.append(((a.get_text() or "").strip(), _abs(base_url, href)))
    for l in soup.find_all("link"):
        href = l.get("href")
        if href:
            text = l.get("title") or l.get("rel") or ""
            if isinstance(text, list):
                text = text[0] if text else ""
            out.append((str(text), _abs(base_url, href)))
    return out

def _select_dataset_links(base_url: str, datasets: Sequence[str]) -> List[Tuple[str, str]]:
    pats = [p.lower() for p in datasets]
    sel, seen = [], set()
    for text, url in _find_links(base_url):
        s = f"{text} {url}".lower()
        if "embebid" in s:                       # evita embebidos
            continue
        if not _looks_archive(s):                 # solo links a comprimidos
            continue
        if pats and not any(p in s for p in pats):
            continue
        if url not in seen:
            seen.add(url)
            sel.append((text, url))
    return sel

def _download(url: str, dest: Path) -> int:
    """Descarga segura con .part + rename atómico. Devuelve bytes."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    last = None
    for i in range(1, DOWNLOAD_RETRIES + 1):
        try:
            with SESSION.get(url, timeout=HTTP_TIMEOUT, stream=True) as r:
                r.raise_for_status()
                tmp = dest.with_suffix(dest.suffix + ".part")
                n = 0
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(2**20):
                        if chunk:
                            f.write(chunk); n += len(chunk)
                tmp.replace(dest)
                return n
        except Exception as exc:
            last = exc
            time.sleep(i)
    raise RuntimeError(f"Descarga fallida: {url} -> {last}")

def _present_in_extraidos(extract_dir: Path, datasets: Sequence[str]) -> Set[str]:
    """Datasets presentes en extraidos/ por coincidencia laxa en filenames."""
    presentes, names = set(), [_canon(p.stem) for p in extract_dir.glob("*") if p.is_file()]
    for ds in datasets:
        if any(_canon(ds) in n for n in names):
            presentes.add(ds)
    return presentes

# --------------------------- Extracción (sin TMP) ---------------------------

def _extract_zip_only_finals(zpath: Path, out_dir: Path) -> List[str]:
    """Extrae SOLO .xlsx/.xls/.csv de un ZIP directo a out_dir."""
    created: List[str] = []
    with zipfile.ZipFile(zpath, "r") as z:
        for m in z.infolist():
            if m.is_dir():
                continue
            ext = Path(m.filename).suffix.lower()
            if ext not in FINAL_EXTS:
                continue
            target = out_dir / _sanitize(Path(m.filename).name)
            base = target; i = 1
            while target.exists():
                target = out_dir / f"{base.stem}__{i}{base.suffix}"; i += 1
            with z.open(m) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
            created.append(str(target))
    return created

def _run(cmd: List[str]) -> Tuple[int, str, str]:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p.returncode, p.stdout.decode("utf-8", "ignore"), p.stderr.decode("utf-8", "ignore")

def _detect_7z(ruta_7z: Optional[str]) -> Optional[str]:
    cands: List[str] = []
    if ruta_7z:
        cands.append(ruta_7z)
    for c in (shutil.which("7z"), shutil.which("7zz"), "/usr/bin/7z", "/usr/bin/7zz"):
        if c:
            cands.append(c)
    if os.name == "nt":
        cands += [r"C:\Program Files\7-Zip\7z.exe", r"C:\Program Files (x86)\7-Zip\7z.exe"]
    for c in cands:
        if c and Path(c).exists():
            return c
    return None

def _extract_rar_with_rarfile(rpath: Path, out_dir: Path) -> Tuple[bool, List[str], str]:
    """Intenta con rarfile+unrar: devuelve (ok, created, reason_if_fail)."""
    try:
        import rarfile
        # Apunta a unrar si existe
        unrar_path = shutil.which("unrar")
        if unrar_path:
            rarfile.UNRAR_TOOL = unrar_path
        else:
            # rarfile puede usar bsdtar/unar como backend en algunas distros
            pass

        created: List[str] = []
        with rarfile.RarFile(rpath) as rf:
            for info in rf.infolist():
                if info.isdir():
                    continue
                ext = Path(info.filename).suffix.lower()
                if ext not in FINAL_EXTS:
                    continue
                target = out_dir / _sanitize(Path(info.filename).name)
                base = target; i = 1
                while target.exists():
                    target = out_dir / f"{base.stem}__{i}{base.suffix}"; i += 1
                # rarfile extrae directo al path indicado
                rf.extract(info, path=out_dir)
                # si el backend crea subcarpeta, mueve al target final
                created_path = out_dir / info.filename
                if created_path.exists() and created_path.is_file():
                    if created_path.resolve() != target.resolve():
                        target.write_bytes(created_path.read_bytes())
                        created_path.unlink(missing_ok=True)
                created.append(str(target))
        return (len(created) > 0, created, "" if created else "rarfile_sin_finales")
    except Exception as exc:
        return False, [], f"rarfile_error:{exc}"

def _extract_with_7z_patterns(apath: Path, out_dir: Path, ruta_7z: Optional[str]) -> Tuple[bool, List[str], str]:
    """
    Extrae SOLO .xlsx/.xls/.csv con 7z/7zz en modo 'e' (plano), directo a out_dir.
    No crea basura porque pasamos patrones.
    """
    exe = _detect_7z(ruta_7z)
    if not exe:
        return False, [], "7z_no_encontrado"

    before = {p.resolve() for p in out_dir.glob("*") if p.is_file()}
    pat = ["*.xlsx", "*.XLSX", "*.xls", "*.XLS", "*.csv", "*.CSV"]
    rc, out, err = _run([exe, "e", "-y", "-aos", str(apath), f"-o{str(out_dir)}"] + pat)
    if rc != 0:
        return False, [], f"7z_rc={rc}:{(err or out).strip()[:200]}"

    after = {p.resolve() for p in out_dir.glob("*") if p.is_file()}
    new_files = sorted(after - before)
    created = [str(p) for p in new_files if Path(p).suffix.lower() in FINAL_EXTS]
    # Si por algún motivo aparece algo que no sea final, elimínalo:
    for p in new_files:
        pp = Path(p)
        if pp.suffix.lower() not in FINAL_EXTS:
            try: pp.unlink()
            except: pass
    return (len(created) > 0, created, "" if created else "7z_sin_finales")

def _extract_archive(apath: Path, out_dir: Path, ruta_7z: Optional[str]) -> Tuple[bool, str, List[str]]:
    """
    Extrae SOLO finales al directorio destino, sin usar tmp.
    Devuelve (ok, msg, created_files).
    """
    if str(apath).endswith(".part"):
        return False, "archivo_incompleto_part", []

    out_dir.mkdir(parents=True, exist_ok=True)
    name = apath.name.lower()

    # ZIP: usar zipfile y filtrar finales
    if name.endswith(".zip"):
        created = _extract_zip_only_finals(apath, out_dir)
        return (len(created) > 0, "extraido" if created else "zip_sin_finales", created)

    # RAR: rarfile+unrar -> fallback 7z
    if name.endswith(".rar"):
        ok, created, reason = _extract_rar_with_rarfile(apath, out_dir)
        if ok:
            return True, "extraido", created
        # fallback a 7z con patrones
        ok2, created2, reason2 = _extract_with_7z_patterns(apath, out_dir, ruta_7z)
        return (ok2, "extraido" if ok2 else (reason or reason2), created2)

    # 7Z: si se presentara, usar 7z con patrones (normalmente no aplica en REMMAQ)
    if name.endswith(".7z"):
        ok, created, reason = _extract_with_7z_patterns(apath, out_dir, ruta_7z)
        return (ok, "extraido" if ok else reason, created)

    return False, f"formato_no_soportado:{apath.suffix}", []

# --------------------------- Main ---------------------------

def main() -> int:
    parser = argparse.ArgumentParser("REMMAQ – Descarga + Extracción (NiFi)")
    parser.add_argument("--indice_web_url", required=True)
    parser.add_argument("--datasets", required=True)               # "CO,NO2,O3,PM2.5,PM10,SO2"
    parser.add_argument("--raw_dir", required=True)                # originales
    parser.add_argument("--extract_dir", required=True)            # extraidos
    parser.add_argument("--logs_dir", required=True)               # logs
    parser.add_argument("--ruta_7z", required=False)               # /usr/bin/7zz | /usr/bin/7z | C:\Program Files\7-Zip\7z.exe
    args = parser.parse_args()

    raw_dir     = Path(args.raw_dir)
    extract_dir = Path(args.extract_dir)
    logs_dir    = Path(args.logs_dir)
    for d in (raw_dir, extract_dir, logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    manifest_path = logs_dir / "extraccion_manifiesto.csv"
    resumen_path  = logs_dir / "extraccion_resumen.json"

    datasets = [x.strip() for x in re.split(r"[;,]", args.datasets) if x.strip()]

    # Si ya están todos los datasets presentes, salir como 'skip'
    presentes = _present_in_extraidos(extract_dir, datasets)
    if presentes == set(datasets) and any(extract_dir.glob("*.xlsx")):
        resumen = {
            "status": "skip",
            "reason": "extraidos_completos",
            "descargados": 0, "extraidos_ok": len(presentes), "errores": 0,
            "raw_dir": str(raw_dir), "extract_dir": str(extract_dir),
            "manifiesto": str(manifest_path), "resumen": str(resumen_path),
            "missing_after_run": [], "last_run": {"started_at": _ts(), "ended_at": _ts(), "status": "skip"},
            "ok_history": []
        }
        resumen_path.write_text(json.dumps(resumen, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(resumen, ensure_ascii=False))
        return 0

    # Seleccionar enlaces del índice SOLO para los faltantes
    pairs = _select_dataset_links(args.indice_web_url, datasets)
    faltantes = [ds for ds in datasets if ds not in presentes]

    plan: List[Dict[str, str]] = []
    for text, url in pairs:
        if not any(_canon(ds) in _canon(f"{text} {url}") for ds in faltantes):
            continue
        base_name = url.split("/")[-1] or (text + ".bin")
        out = raw_dir / _sanitize(base_name)
        plan.append({"dataset": text, "file_url": url, "out_path": str(out)})

    # Manifiesto (header si no existe)
    if not manifest_path.exists():
        with open(manifest_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["dataset","file_url","out_path","status","bytes","timestamp"])
            w.writeheader()

    descargados = 0
    extraidos_ok = 0
    errores = 0
    fallidos: List[Dict[str, str]] = []
    ok_files: List[Dict[str, str]] = []
    started = _ts()

    # ------------------ FASE 1: DESCARGAR TODO ------------------
    for row in plan:
        dest = Path(row["out_path"])
        if dest.exists():
            with open(manifest_path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["dataset","file_url","out_path","status","bytes","timestamp"])
                w.writerow({**row, "status":"ya_existia","bytes":dest.stat().st_size,"timestamp":_ts()})
            continue
        try:
            nbytes = _download(row["file_url"], dest)
            descargados += 1
            with open(manifest_path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["dataset","file_url","out_path","status","bytes","timestamp"])
                w.writerow({**row, "status":"descargado","bytes":nbytes,"timestamp":_ts()})
        except Exception as exc:
            errores += 1
            fallidos.append({"dataset":row["dataset"], "archivo":dest.name, "motivo":f"error_descarga:{exc}"})
            with open(manifest_path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["dataset","file_url","out_path","status","bytes","timestamp"])
                w.writerow({**row, "status":f"error_descarga:{exc}", "bytes":0, "timestamp":_ts()})

    # ------------------ FASE 2: EXTRAER TODO ------------------
    for row in plan:
        arc = Path(row["out_path"])
        if not arc.exists() or str(arc).endswith(".part"):
            continue

        # Si ya existe un final para este dataset en extraidos, saltar
        if any(_canon(row["dataset"]) in _canon(p.stem) for p in extract_dir.glob("*") if p.is_file()):
            continue

        ok = False; msg = ""; created: List[str] = []
        for j in range(1, EXTRACT_RETRIES + 1):
            ok, msg, created = _extract_archive(arc, extract_dir, args.ruta_7z)
            if ok:
                break
            time.sleep(j * 0.5)

        if ok:
            extraidos_ok += 1
            # borrar comprimido
            try: arc.unlink(missing_ok=True)
            except: pass
            with open(manifest_path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["dataset","file_url","out_path","status","bytes","timestamp"])
                w.writerow({**row, "status":"extraido","bytes":arc.stat().st_size if arc.exists() else 0,"timestamp":_ts()})
            for c in created:
                ok_files.append({"dataset":row["dataset"],"file":c})
        else:
            errores += 1
            fallidos.append({"dataset":row["dataset"], "archivo":arc.name, "motivo":msg})
            with open(manifest_path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["dataset","file_url","out_path","status","bytes","timestamp"])
                w.writerow({**row, "status":msg, "bytes":arc.stat().st_size if arc.exists() else 0, "timestamp":_ts()})

    # Estado final
    presentes_final = _present_in_extraidos(extract_dir, datasets)
    missing_after   = [ds for ds in datasets if ds not in presentes_final]
    ended = _ts()
    status_final = "ok" if (errores == 0 and not missing_after) else ("error" if errores > 0 else "incompleto")

    # Historial (append)
    ok_history: List[Dict[str, str]] = []
    try:
        prev = json.loads((logs_dir / "extraccion_resumen.json").read_text(encoding="utf-8"))
        ok_history = prev.get("ok_history", [])
    except Exception:
        ok_history = []
    for it in ok_files:
        ok_history.append({"dataset": it["dataset"], "file": it["file"], "when": ended})

    resumen = {
        "status": status_final,
        "descargados": descargados,
        "extraidos_ok": len(presentes) + extraidos_ok,
        "errores": errores,
        "raw_dir": str(raw_dir),
        "extract_dir": str(extract_dir),
        "manifiesto": str(manifest_path),
        "resumen": str(resumen_path),
        "missing_after_run": missing_after,
        "last_run": {"started_at": started, "ended_at": ended, "status": status_final,
                     "successes": ok_files, "failures": fallidos},
        "ok_history": ok_history,
    }

    resumen_path.write_text(json.dumps(resumen, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(resumen, ensure_ascii=False))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status":"error","reason":f"fatal:{exc.__class__.__name__}","message":str(exc),"ts":_ts()}, ensure_ascii=False))
        sys.exit(1)