#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL_REMMAQ_LOAD.py

Trunca e inserta los JSON normalizados en MongoDB Atlas:

- remmaq_analitico.json         -> colección remmaq_analitico
- remmaq_geo_estaciones.json    -> colección remmaq_estaciones

NiFi solo ejecuta este script y evalúa el JSON que se imprime en STDOUT.
Logs de detalle se envían a STDERR.
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import PyMongoError


def leer_jsonl(path: Path) -> list[dict]:
    """
    Lee un archivo JSONL (un documento JSON por línea) y
    devuelve una lista de diccionarios.
    """
    docs: list[dict] = []

    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                docs.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(
                    f"[WARN] Línea {i} de {path.name} no es JSON válido: {e}",
                    file=sys.stderr,
                )

    return docs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trunca y carga JSON REMMAQ en MongoDB Atlas"
    )

    parser.add_argument(
        "--dir",
        required=True,
        help="Directorio donde están los JSON normalizados",
    )
    parser.add_argument(
        "--mongo-uri",
        required=True,
        help="MongoDB connection string (MongoDB Atlas URI)",
    )
    parser.add_argument(
        "--db",
        default="remmaq",
        help="Nombre de la base de datos en MongoDB (por defecto: remmaq)",
    )
    parser.add_argument(
        "--col-analitico",
        default="remmaq_analitico",
        help="Colección para datos analíticos",
    )
    parser.add_argument(
        "--col-estaciones",
        default="remmaq_estaciones",
        help="Colección para dimensión de estaciones",
    )
    parser.add_argument(
        "--file-analitico",
        default="remmaq_analitico.json",
        help="Nombre del archivo JSON analítico",
    )
    parser.add_argument(
        "--file-estaciones",
        default="remmaq_geo_estaciones.json",
        help="Nombre del archivo JSON de estaciones",
    )

    args = parser.parse_args()

    base_dir = Path(args.dir)
    file_analitico = base_dir / args.file_analitico
    file_estaciones = base_dir / args.file_estaciones

    # Validamos existencia de archivos
    if not file_analitico.exists():
        resultado = {
            "estado": "ERROR",
            "mensaje": f"No se encuentra el archivo analítico: {file_analitico}",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(json.dumps(resultado, ensure_ascii=False))
        sys.exit(1)

    if not file_estaciones.exists():
        resultado = {
            "estado": "ERROR",
            "mensaje": f"No se encuentra el archivo de estaciones: {file_estaciones}",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(json.dumps(resultado, ensure_ascii=False))
        sys.exit(1)

    try:
        client = MongoClient(args.mongo_uri)
        db = client[args.db]

        col_analitico = db[args.col_analitico]
        col_estaciones = db[args.col_estaciones]

        # -----------------------------
        # 1) Truncar colecciones
        # -----------------------------
        print(
            f"[INFO] Truncando colecciones {args.col_analitico} y {args.col_estaciones}...",
            file=sys.stderr,
        )

        res_del_analitico = col_analitico.delete_many({})
        res_del_estaciones = col_estaciones.delete_many({})

        borrados_analitico = res_del_analitico.deleted_count
        borrados_estaciones = res_del_estaciones.deleted_count

        # -----------------------------
        # 2) Cargar JSON analítico
        # -----------------------------
        print(
            f"[INFO] Leyendo {file_analitico} ...",
            file=sys.stderr,
        )
        docs_analitico = leer_jsonl(file_analitico)
        insertados_analitico = 0

        if docs_analitico:
            res_ins_analitico = col_analitico.insert_many(
                docs_analitico, ordered=False
            )
            insertados_analitico = len(res_ins_analitico.inserted_ids)

        # -----------------------------
        # 3) Cargar JSON estaciones
        # -----------------------------
        print(
            f"[INFO] Leyendo {file_estaciones} ...",
            file=sys.stderr,
        )
        docs_estaciones = leer_jsonl(file_estaciones)
        insertados_estaciones = 0

        if docs_estaciones:
            res_ins_estaciones = col_estaciones.insert_many(
                docs_estaciones, ordered=False
            )
            insertados_estaciones = len(res_ins_estaciones.inserted_ids)

        # -----------------------------
        # 4) Borrar archivos JSON tras la carga
        # -----------------------------
        borrado_analitico_ok = False
        borrado_estaciones_ok = False

        try:
            file_analitico.unlink()
            borrado_analitico_ok = True
            print(
                f"[INFO] Archivo eliminado: {file_analitico}",
                file=sys.stderr,
            )
        except OSError as e:
            print(
                f"[WARN] No se pudo borrar {file_analitico}: {e}",
                file=sys.stderr,
            )

        try:
            file_estaciones.unlink()
            borrado_estaciones_ok = True
            print(
                f"[INFO] Archivo eliminado: {file_estaciones}",
                file=sys.stderr,
            )
        except OSError as e:
            print(
                f"[WARN] No se pudo borrar {file_estaciones}: {e}",
                file=sys.stderr,
            )

        # -----------------------------
        # 5) Resultado para NiFi (STDOUT)
        # -----------------------------
        resultado = {
            "estado": "OK",
            "mensaje": "Carga en MongoDB completada",
            "db": args.db,
            "coleccion_analitico": args.col_analitico,
            "coleccion_estaciones": args.col_estaciones,
            "borrados_analitico": borrados_analitico,
            "borrados_estaciones": borrados_estaciones,
            "insertados_analitico": insertados_analitico,
            "insertados_estaciones": insertados_estaciones,
            "json_borrado_analitico": borrado_analitico_ok,
            "json_borrado_estaciones": borrado_estaciones_ok,
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(json.dumps(resultado, ensure_ascii=False))
        sys.exit(0)

    except PyMongoError as e:
        print(f"[ERROR] PyMongoError: {e}", file=sys.stderr)
        resultado = {
            "estado": "ERROR",
            "mensaje": f"Error en carga MongoDB: {e}",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(json.dumps(resultado, ensure_ascii=False))
        sys.exit(1)

    except Exception as e:
        print(f"[ERROR] Error inesperado: {e}", file=sys.stderr)
        resultado = {
            "estado": "ERROR",
            "mensaje": f"Error inesperado: {e}",
            "timestamp": datetime.utcnow().isoformat(),
        }
        print(json.dumps(resultado, ensure_ascii=False))
        sys.exit(1)



if __name__ == "__main__":
    main()