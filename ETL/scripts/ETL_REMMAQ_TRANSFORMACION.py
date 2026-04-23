#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
REMMAQ - Generación de modelo analítico (JSONL) para NiFi / MongoDB

Este script prioriza integridad analítica (faltante != 0) y, opcionalmente, ofrece
un modo "BI-safe" para evitar NULLs en campos numéricos sin introducir sesgos
silenciosos (se añaden flags de trazabilidad).

Salidas:
- remmaq_analitico.json (JSONL)
- remmaq_geo_estaciones.json (JSONL)

Parámetros clave:
- --start / --end : ventana temporal. El modelo registra periodo calendario y ventana efectiva.
- --include_diario : exporta DIARIO (más volumen).
- --no_nulls : fuerza salida SIN NULL en métricas numéricas (BI-safe) y añade banderas:
    - tiene_dato (bool)            -> diferencia 0 real vs 0 por ausencia de dato
    - tiene_geoloc (bool)          -> diferencia coords reales vs sentinel
    - valor_sin_dato (bool)        -> indica ausencia de dato para el registro
"""

import argparse
import calendar
from datetime import datetime
import json
from pathlib import Path
import re
import sys
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import geopandas as gpd

# ---------------------------------------------------------------------
# Versión / trazabilidad
# ---------------------------------------------------------------------

SCHEMA_VERSION = "3.0.0"
CRITERIO_CALIDAD_VERSION = "1.1"
GENERATED_BY = "ETL_REMMAQ_TRANSFORMACION"

# ---------------------------------------------------------------------
# Configuración general
# ---------------------------------------------------------------------

CIUDAD = "Quito"

ARCHIVOS_POR_CONTAMINANTE = {
    "CO": "CO.xlsx",
    "NO2": "NO2.xlsx",
    "O3": "O3.xlsx",
    "PM10": "PM10.xlsx",
    "PM2.5": "PM2.5.xlsx",
    "SO2": "SO2.xlsx",
}

ESTACION_ALIASES = {
    "EL CAMAL": "CAMAL",
    "CAMAL": "CAMAL",
}

CONFIG_CONTAMINANTES: Dict[str, Dict] = {
    "CO": {
        "unidad": "mg/m3",
        "nombre_completo": "Monóxido de carbono (CO)",
        "metrica_diaria": "mean_24h",
        "umbral_diario": 4.0,
        "umbral_anual": None,
        "min_horas_dia": 18,
        "min_estaciones_ciudad": 3,
        "ciudad_modo": "estaciones_validas",
    },
    "NO2": {
        "unidad": "µg/m3",
        "nombre_completo": "Dióxido de nitrógeno (NO₂)",
        "metrica_diaria": "mean_24h",
        "umbral_diario": 25.0,
        "umbral_anual": 10.0,
        "min_horas_dia": 18,
        "min_estaciones_ciudad": 3,
        "ciudad_modo": "estaciones_validas",
    },
    "O3": {
        "unidad": "µg/m3",
        "nombre_completo": "Ozono troposférico (O₃)",
        "metrica_diaria": "max_8h",
        "umbral_diario": 100.0,
        "umbral_anual": None,
        "min_horas_dia": 18,
        "min_estaciones_ciudad": 3,
        "ciudad_modo": "estaciones_validas",
        "ventana_horas": 8,
        "min_horas_ventana": 6,
        "min_ventanas_dia": 18,
    },
    "PM10": {
        "unidad": "µg/m3",
        "nombre_completo": "Material particulado grueso (PM₁₀)",
        "metrica_diaria": "mean_24h",
        "umbral_diario": 45.0,
        "umbral_anual": 15.0,
        "min_horas_dia": 18,
        "min_estaciones_ciudad": 3,
        "ciudad_modo": "estaciones_validas",
    },
    "PM2.5": {
        "unidad": "µg/m3",
        "nombre_completo": "Material particulado fino (PM₂.₅)",
        "metrica_diaria": "mean_24h",
        "umbral_diario": 15.0,
        "umbral_anual": 5.0,
        "min_horas_dia": 18,
        "min_estaciones_ciudad": 3,
        "ciudad_modo": "estaciones_validas",
    },
    "SO2": {
        "unidad": "µg/m3",
        "nombre_completo": "Dióxido de azufre (SO₂)",
        "metrica_diaria": "mean_24h",
        "umbral_diario": 40.0,
        "umbral_anual": None,
        "min_horas_dia": 18,
        "min_estaciones_ciudad": 3,
        "ciudad_modo": "estaciones_validas",
    },
}

RANGOS_PLAUSIBLES = {
    "CO": (0, 50),       # mg/m3
    "NO2": (0, 500),     # µg/m3
    "O3": (0, 500),      # µg/m3
    "PM10": (0, 1000),   # µg/m3
    "PM2.5": (0, 1000),  # µg/m3
    "SO2": (0, 1000),    # µg/m3
}

# Coordenadas aproximadas (solo si falta en shapefile)
COORDS_APROX = {
    "CONDADO":     (-0.104962,  -78.500011),
    "SAN ANTONIO": (-0.0107704, -78.4480136),
    "TURUBAMBA":   (-0.3361839, -78.5323747),
    "CHILLOGALLO": (-0.27595,   -78.55397),
}

DEFAULT_MIN_HORAS_DIA = 18
DEFAULT_MIN_ESTACIONES_CIUDAD = 3
DEFAULT_CIUDAD_MODO = "estaciones_validas"
DEFAULT_VENTANA_8H = 8
DEFAULT_MIN_HORAS_VENTANA_8H = 6
DEFAULT_MIN_VENTANAS_DIA_8H = 18

# Sentinels BI-safe (para evitar NULLs sin poner coordenadas reales)
SENTINEL_FLOAT = 0.0
SENTINEL_GEO = -999.0  # evita caer en (0,0) en mapas si olvidan filtrar

# ---------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------

def clasificar_periodo_anual(anio: int) -> str:
    if 2015 <= anio <= 2019:
        return "Normalidad previa"
    if 2020 <= anio <= 2021:
        return "Cambio de movilidad"
    if anio >= 2022:
        return "Nueva normalidad"
    return "Fuera de ventana"


def normalizar_nombre_estacion(nombre: str) -> str:
    s = str(nombre).upper().strip()
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s)
    return ESTACION_ALIASES.get(s, s)


def _safe_float(v, default=None):
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        f = float(v)
        import math
        if not math.isfinite(f):
            return default
        return f
    except Exception:
        return default


def _safe_int(v, default=0) -> int:
    if v is None:
        return int(default)
    try:
        if pd.isna(v):
            return int(default)
    except Exception:
        pass
    try:
        return int(v)
    except Exception:
        return int(default)


def _safe_bool(v, default=False) -> bool:
    if v is None:
        return bool(default)
    try:
        if pd.isna(v):
            return bool(default)
    except Exception:
        pass
    try:
        return bool(v)
    except Exception:
        return bool(default)


def _emit_float(v, no_nulls: bool, default_if_null: float = SENTINEL_FLOAT):
    x = _safe_float(v, default=None)
    if x is None:
        return default_if_null if no_nulls else None
    return x


def _emit_pct(v, no_nulls: bool):
    # pct fuera de [0,100] se normaliza a None (o sentinel si no_nulls)
    x = _safe_float(v, default=None)
    if x is None:
        return 0.0 if no_nulls else None
    if x < 0 or x > 100:
        return 0.0 if no_nulls else None
    return float(round(x, 1))


def _normalizar_fecha_fin_inclusiva(end_str: Optional[str]) -> Optional[pd.Timestamp]:
    """Si end viene como fecha (00:00:00), lo hace inclusivo hasta fin de día."""
    if not end_str:
        return None
    tmp = pd.to_datetime(end_str)
    if tmp.time() == datetime.min.time():
        return tmp + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    return tmp


def _periodo_boundaries(anio: int, mes: Optional[int]) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Devuelve inicio/fin calendario del periodo (incluye fin de día)."""
    anio = int(anio)
    if mes is None:
        start = pd.Timestamp(year=anio, month=1, day=1)
        end = pd.Timestamp(year=anio, month=12, day=31, hour=23, minute=59, second=59)
        return start, end
    mes = int(mes)
    last_day = calendar.monthrange(anio, mes)[1]
    start = pd.Timestamp(year=anio, month=mes, day=1)
    end = pd.Timestamp(year=anio, month=mes, day=last_day, hour=23, minute=59, second=59)
    return start, end


def _interseccion_periodo(
    anio: int,
    mes: Optional[int],
    ventana_inicio: Optional[pd.Timestamp],
    ventana_fin: Optional[pd.Timestamp],
) -> Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp, int, int]:
    """
    Devuelve:
    - inicio_periodo, fin_periodo (calendario del mes/año)
    - inicio_efectivo, fin_efectivo (intersección con ventana)
    - dias_calendario_periodo, dias_calendario_efectivo
    """
    p_start, p_end = _periodo_boundaries(anio, mes)

    e_start = p_start if ventana_inicio is None else max(p_start, ventana_inicio)
    e_end = p_end if ventana_fin is None else min(p_end, ventana_fin)

    # Días calendario del periodo completo
    if mes is None:
        dias_periodo = 366 if calendar.isleap(int(anio)) else 365
    else:
        dias_periodo = calendar.monthrange(int(anio), int(mes))[1]

    # Días efectivos
    if e_end < e_start:
        dias_eff = 0
    else:
        dias_eff = (e_end.floor("D") - e_start.floor("D")).days + 1

    return p_start, p_end, e_start, e_end, dias_periodo, dias_eff


def _estado_anual_oms(aplica_umbral: bool, supera: bool, tiene_dato: bool) -> str:
    if not tiene_dato:
        return "SIN_DATO"
    if not aplica_umbral:
        return "SIN_UMBRAL"
    return "EXCEDE" if supera else "CUMPLE"


def _estado_diario_oms(pct_dias_exc) -> str:
    v = _safe_float(pct_dias_exc, default=None)
    if v is None:
        return "SIN_DATO"
    if v <= 0:
        return "SIN_EXCEDENCIAS"
    if v <= 5:
        return "EXCEDENCIAS_BAJAS"
    if v <= 15:
        return "EXCEDENCIAS_MODERADAS"
    return "EXCEDENCIAS_ALTAS"


def _calidad_dato(pct_sin_dato, pct_cero) -> str:
    ps = _safe_float(pct_sin_dato, default=None)
    pc = _safe_float(pct_cero, default=None)
    if ps is None:
        return "BAJA"
    pc_eval = 0.0 if pc is None else pc
    if ps <= 5 and pc_eval <= 5:
        return "ALTA"
    if ps <= 20:
        return "MEDIA"
    return "BAJA"


def _generar_codigos_estacion_unicos(estaciones: List[str]) -> Dict[str, str]:
    cleaned = []
    for s in estaciones:
        s2 = re.sub(r"[^A-Z0-9]+", "", s.upper())
        base = (s2[:6] if s2 else "EST")
        cleaned.append((s, base))

    cleaned.sort(key=lambda x: x[0])

    counts: Dict[str, int] = {}
    mapping: Dict[str, str] = {}
    for est, base in cleaned:
        n = counts.get(base, 0) + 1
        counts[base] = n
        mapping[est] = base if n == 1 else f"{base}_{n}"
    return mapping


# ---------------------------------------------------------------------
# Lectura y preprocesamiento de datos horarios
# ---------------------------------------------------------------------

def leer_horario_xlsx(path: Path, contaminante: Optional[str] = None) -> pd.DataFrame:
    """
    Lee archivo horario REMMAQ en Excel (formato nuevo o antiguo) y devuelve:
    - 'FECHA_HORA' (datetime)
    - columnas de estaciones en float (preservando NaN como "sin medición")
    """
    df = pd.read_excel(path)

    # Detectar columna de fecha/hora
    fecha_col = None
    for c in df.columns:
        if "FECHA" in str(c).upper():
            fecha_col = c
            break
    if fecha_col is None:
        fecha_col = df.columns[0]
    df = df.rename(columns={fecha_col: "FECHA_HORA"})

    # Si la primera fila contiene metadata tipo "FECHA - UNIDAD", descartarla
    valor_fila0 = str(df["FECHA_HORA"].iloc[0]).upper()
    if "FECHA" in valor_fila0 and "UNIDAD" in valor_fila0:
        df = df.iloc[1:].copy()

    df["FECHA_HORA"] = pd.to_datetime(df["FECHA_HORA"], errors="coerce")
    df = df.dropna(subset=["FECHA_HORA"])

    # Normalizar nombres de estaciones y consolidar duplicados tras renombrado
    estaciones_cols = [c for c in df.columns if c != "FECHA_HORA"]
    rename_map = {c: normalizar_nombre_estacion(c) for c in estaciones_cols}
    df = df.rename(columns=rename_map)

    cols_no_fecha = [c for c in df.columns if c != "FECHA_HORA"]
    df_out = df[["FECHA_HORA"]].copy()

    for col in sorted(set(cols_no_fecha)):
        vals = df.loc[:, col]
        if isinstance(vals, pd.DataFrame):
            df_out[col] = vals.mean(axis=1, skipna=True)
        else:
            df_out[col] = vals

    df = df_out

    estaciones_cols = [c for c in df.columns if c != "FECHA_HORA"]

    # Normalizar strings de faltantes antes de convertir a numérico
    missing_tokens = {"NA", "N/A", "NAN", "NULL", "-", "--", ""}
    df[estaciones_cols] = df[estaciones_cols].replace(
        {t: np.nan for t in missing_tokens}
    )
    df[estaciones_cols] = df[estaciones_cols].applymap(
        lambda x: np.nan if isinstance(x, str) and x.strip().upper() in missing_tokens else x
    )

    df[estaciones_cols] = df[estaciones_cols].apply(pd.to_numeric, errors="coerce")

    # Reglas de saneamiento:
    # - faltante se mantiene como NaN (NO imputar 0)
    # - negativos se tratan como inválidos -> NaN
    df[estaciones_cols] = df[estaciones_cols].mask(df[estaciones_cols] < 0)
    # Filtrado de rangos plausibles (reduce outliers por errores de captura/sensor)
    if contaminante and contaminante in RANGOS_PLAUSIBLES:
        vmin, vmax = RANGOS_PLAUSIBLES[contaminante]
        df[estaciones_cols] = df[estaciones_cols].mask((df[estaciones_cols] < vmin) | (df[estaciones_cols] > vmax))


    # Eliminar duplicados exactos de timestamp (si existieran) quedándonos con el promedio
    if df["FECHA_HORA"].duplicated().any():
        df = df.groupby("FECHA_HORA", as_index=False)[estaciones_cols].mean()

    return df


def construir_df_diario(
    df_horas: pd.DataFrame,
    contaminante: str,
    fecha_inicio: Optional[pd.Timestamp],
    fecha_fin: Optional[pd.Timestamp],
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Agrega datos horarios a nivel diario por estación y CIUDAD, con reglas de completitud.
    Retorna:
      - df_diario (una fila por día)
      - estaciones_cols (lista de estaciones)
    """
    df = df_horas.copy()
    estaciones_cols = [c for c in df.columns if c != "FECHA_HORA"]

    cfg = CONFIG_CONTAMINANTES.get(contaminante, {})
    min_horas_dia = int(cfg.get("min_horas_dia", DEFAULT_MIN_HORAS_DIA))
    min_est_ciudad = int(cfg.get("min_estaciones_ciudad", DEFAULT_MIN_ESTACIONES_CIUDAD))
    metrica_diaria = cfg.get("metrica_diaria", "mean_24h")
    ciudad_modo = cfg.get("ciudad_modo", DEFAULT_CIUDAD_MODO)

    ventana_horas = int(cfg.get("ventana_horas", DEFAULT_VENTANA_8H))
    min_horas_ventana = int(cfg.get("min_horas_ventana", DEFAULT_MIN_HORAS_VENTANA_8H))
    min_ventanas_dia = int(cfg.get("min_ventanas_dia", DEFAULT_MIN_VENTANAS_DIA_8H))

    # Filtro temporal (con padding para max_8h)
    if fecha_inicio is not None:
        start_pad = fecha_inicio - pd.Timedelta(hours=ventana_horas - 1) if metrica_diaria == "max_8h" else fecha_inicio
        df = df[df["FECHA_HORA"] >= start_pad]
    if fecha_fin is not None:
        df = df[df["FECHA_HORA"] <= fecha_fin]

    df["FECHA_DIA"] = df["FECHA_HORA"].dt.floor("D")

    if metrica_diaria == "mean_24h":
        mean_d = df.groupby("FECHA_DIA")[estaciones_cols].mean()
        count_d = df.groupby("FECHA_DIA")[estaciones_cols].count()
        df_d = mean_d.reset_index()
        for col in estaciones_cols:
            df_d.loc[count_d[col].values < min_horas_dia, col] = np.nan

    elif metrica_diaria == "max_8h":
        df_idx = df.sort_values("FECHA_HORA").set_index("FECHA_HORA")
        roll_mean = (
            df_idx[estaciones_cols]
            .rolling(f"{ventana_horas}h", min_periods=min_horas_ventana)
            .mean()
            .reset_index()
        )
        roll_mean["FECHA_DIA"] = roll_mean["FECHA_HORA"].dt.floor("D")

        max_8h_d = roll_mean.groupby("FECHA_DIA")[estaciones_cols].max()
        ventanas_count = roll_mean.groupby("FECHA_DIA")[estaciones_cols].count()
        horas_count = df.groupby("FECHA_DIA")[estaciones_cols].count()

        df_d = max_8h_d.reset_index()
        for col in estaciones_cols:
            df_d.loc[horas_count[col].values < min_horas_dia, col] = np.nan
            df_d.loc[ventanas_count[col].values < min_ventanas_dia, col] = np.nan
    else:
        raise ValueError(f"Métrica diaria desconocida para {contaminante}: {metrica_diaria}")

    # Recorte diario al rango solicitado
    if fecha_inicio is not None:
        df_d = df_d[df_d["FECHA_DIA"] >= fecha_inicio.floor("D")]
    if fecha_fin is not None:
        df_d = df_d[df_d["FECHA_DIA"] <= fecha_fin.floor("D")]

    # CIUDAD diaria
    n_est_validas = df_d[estaciones_cols].notna().sum(axis=1)
    df_d["ESTACIONES_VALIDAS_DIA"] = n_est_validas.astype("Int64")

    # Siempre calcular CIUDAD (promedio de estaciones disponibles) y luego aplicar regla
    # de mínimo de estaciones válidas. Si no se cumple, CIUDAD queda como NaN.
    if estaciones_cols:
        df_d["CIUDAD"] = df_d[estaciones_cols].mean(axis=1, skipna=True)
    else:
        df_d["CIUDAD"] = np.nan

    # ciudad_modo se mantiene por compatibilidad; actualmente ambos modos usan promedio
    # (puedes extender aquí a ponderaciones/denominador fijo si se requiere).
    if ciudad_modo not in ("estaciones_validas", "den_fijo"):
        # fallback seguro
        ciudad_modo = "estaciones_validas"

    df_d.loc[n_est_validas < min_est_ciudad, "CIUDAD"] = np.nan

    df_d["ANIO"] = df_d["FECHA_DIA"].dt.year
    df_d["MES"] = df_d["FECHA_DIA"].dt.month
    df_d["PERIODO"] = df_d["ANIO"].apply(clasificar_periodo_anual)

    return df_d, estaciones_cols


# ---------------------------------------------------------------------
# Modelos anual / mensual (+ opcional diario)
# ---------------------------------------------------------------------

def modelos_para_contaminante(
    nombre: str,
    df_d: pd.DataFrame,
    estaciones_cols: List[str],
):
    cfg = CONFIG_CONTAMINANTES[nombre]
    umbral_d = float(cfg["umbral_diario"])
    umbral_a = cfg.get("umbral_anual", None)
    aplica_anual = (umbral_a is not None) and (_safe_float(umbral_a, None) is not None) and (float(umbral_a) > 0)



    # -------------------------------
    # A) CIUDAD - ANUAL
    # -------------------------------
    grp_ci_anual = df_d.groupby("ANIO")["CIUDAD"]
    grp_rep_anual = df_d.groupby("ANIO")["ESTACIONES_VALIDAS_DIA"]

    dias_totales_ci = grp_ci_anual.count()
    prom_anual_ci = grp_ci_anual.mean()
    max_diario_ci = grp_ci_anual.max()
    dias_cero_ci = grp_ci_anual.apply(lambda s: (s == 0).sum())
    dias_exc_ci = grp_ci_anual.apply(lambda s: (s > umbral_d).sum())

    pct_cero_ci = (dias_cero_ci / dias_totales_ci.replace(0, np.nan) * 100).round(1)
    pct_exc_ci = (dias_exc_ci / dias_totales_ci.replace(0, np.nan) * 100).round(1)

    rep_prom = grp_rep_anual.mean().round(2)
    rep_min = grp_rep_anual.min()
    rep_max = grp_rep_anual.max()

    modelo_ciudad_anual = pd.DataFrame({
        "CIUDAD": CIUDAD,
        "CONTAMINANTE": nombre,
        "ANIO": prom_anual_ci.index.astype(int),
        "PERIODO": prom_anual_ci.index.to_series().apply(clasificar_periodo_anual).values,
        "PROM_ANUAL": prom_anual_ci.values,
        "MAX_DIARIO": max_diario_ci.values,
        "UMBRAL_ANUAL": (np.full_like(prom_anual_ci.values, float(umbral_a), dtype="float64") if aplica_anual else np.full_like(prom_anual_ci.values, np.nan, dtype="float64")),
        "SUPERA_OMS_ANUAL": (prom_anual_ci.values > float(umbral_a)) if aplica_anual else np.zeros_like(prom_anual_ci.values, dtype=bool),
        "APLICA_UMBRAL_ANUAL": np.ones_like(prom_anual_ci.values, dtype=bool) if aplica_anual else np.zeros_like(prom_anual_ci.values, dtype=bool),
        "DIAS_CERO": dias_cero_ci.values.astype(int),
        "PCT_CERO": pct_cero_ci.values,
        "DIAS_EXC": dias_exc_ci.values.astype(int),
        "DIAS_TOTALES": dias_totales_ci.values.astype(int),
        "PCT_DIAS_EXC": pct_exc_ci.values,
        "UMBRAL_DIARIO": umbral_d,
        "ESTACIONES_VALIDAS_PROM": rep_prom.values,
        "ESTACIONES_VALIDAS_MIN": rep_min.values.astype(int),
        "ESTACIONES_VALIDAS_MAX": rep_max.values.astype(int),
    })

    modelo_ciudad_anual = modelo_ciudad_anual[modelo_ciudad_anual["DIAS_TOTALES"] > 0].reset_index(drop=True)


    # -------------------------------
    # B) ESTACIONES - ANUAL
    # -------------------------------
    df_long = df_d.melt(
        id_vars=["FECHA_DIA", "ANIO", "MES", "PERIODO"],
        value_vars=estaciones_cols,
        var_name="ESTACION",
        value_name="VALOR",
    )

    grp_est_anual = df_long.groupby(["ANIO", "ESTACION"])["VALOR"]
    dias_totales_est = grp_est_anual.count()
    prom_anual_est = grp_est_anual.mean()
    max_diario_est = grp_est_anual.max()
    dias_cero_est = grp_est_anual.apply(lambda s: (s == 0).sum())
    dias_exc_est = grp_est_anual.apply(lambda s: (s > umbral_d).sum())

    pct_cero_est = (dias_cero_est / dias_totales_est.replace(0, np.nan) * 100).round(1)
    pct_exc_est = (dias_exc_est / dias_totales_est.replace(0, np.nan) * 100).round(1)

    modelo_estaciones_anual = pd.DataFrame({
        "CIUDAD": CIUDAD,
        "CONTAMINANTE": nombre,
        "ANIO": prom_anual_est.index.get_level_values("ANIO").astype(int),
        "ESTACION": prom_anual_est.index.get_level_values("ESTACION"),
        "PERIODO": prom_anual_est.index.get_level_values("ANIO").to_series().apply(clasificar_periodo_anual).values,
        "PROM_ANUAL": prom_anual_est.values,
        "MAX_DIARIO": max_diario_est.values,
        "UMBRAL_ANUAL": (np.full_like(prom_anual_est.values, float(umbral_a), dtype="float64") if aplica_anual else np.full_like(prom_anual_est.values, np.nan, dtype="float64")),
        "SUPERA_OMS_ANUAL": (prom_anual_est.values > float(umbral_a)) if aplica_anual else np.zeros_like(prom_anual_est.values, dtype=bool),
        "APLICA_UMBRAL_ANUAL": np.ones_like(prom_anual_est.values, dtype=bool) if aplica_anual else np.zeros_like(prom_anual_est.values, dtype=bool),
        "DIAS_CERO": dias_cero_est.values.astype(int),
        "PCT_CERO": pct_cero_est.values,
        "DIAS_EXC": dias_exc_est.values.astype(int),
        "DIAS_TOTALES": dias_totales_est.values.astype(int),
        "PCT_DIAS_EXC": pct_exc_est.values,
        "UMBRAL_DIARIO": umbral_d,
    })

    modelo_estaciones_anual = modelo_estaciones_anual[modelo_estaciones_anual["DIAS_TOTALES"] > 0].reset_index(drop=True)


    # -------------------------------
    # C) CIUDAD - MENSUAL
    # -------------------------------
    grp_ci_m = df_d.groupby(["ANIO", "MES"])["CIUDAD"]
    grp_rep_m = df_d.groupby(["ANIO", "MES"])["ESTACIONES_VALIDAS_DIA"]

    dias_totales_ci_m = grp_ci_m.count()
    prom_m_ci = grp_ci_m.mean()
    max_diario_ci_m = grp_ci_m.max()
    dias_cero_ci_m = grp_ci_m.apply(lambda s: (s == 0).sum())
    dias_exc_ci_m = grp_ci_m.apply(lambda s: (s > umbral_d).sum())

    pct_cero_ci_m = (dias_cero_ci_m / dias_totales_ci_m.replace(0, np.nan) * 100).round(1)
    pct_exc_ci_m = (dias_exc_ci_m / dias_totales_ci_m.replace(0, np.nan) * 100).round(1)

    rep_prom_m = grp_rep_m.mean().round(2)
    rep_min_m = grp_rep_m.min()
    rep_max_m = grp_rep_m.max()

    modelo_ciudad_mensual = pd.DataFrame({
        "CIUDAD": CIUDAD,
        "CONTAMINANTE": nombre,
        "ANIO": prom_m_ci.index.get_level_values("ANIO").astype(int),
        "MES": prom_m_ci.index.get_level_values("MES").astype(int),
        "PERIODO": prom_m_ci.index.get_level_values("ANIO").to_series().apply(clasificar_periodo_anual).values,
        "PROM_MENSUAL": prom_m_ci.values,
        "MAX_DIARIO_MES": max_diario_ci_m.values,
        "DIAS_TOTALES_MES": dias_totales_ci_m.values.astype(int),
        "DIAS_CERO_MES": dias_cero_ci_m.values.astype(int),
        "PCT_CERO_MES": pct_cero_ci_m.values,
        "DIAS_EXC_MES": dias_exc_ci_m.values.astype(int),
        "PCT_DIAS_EXC_MES": pct_exc_ci_m.values,
        "UMBRAL_DIARIO": umbral_d,
        "ESTACIONES_VALIDAS_PROM": rep_prom_m.values,
        "ESTACIONES_VALIDAS_MIN": rep_min_m.values.astype(int),
        "ESTACIONES_VALIDAS_MAX": rep_max_m.values.astype(int),
    })

    modelo_ciudad_mensual = modelo_ciudad_mensual[modelo_ciudad_mensual["DIAS_TOTALES_MES"] > 0].reset_index(drop=True)


    # -------------------------------
    # D) ESTACIONES - MENSUAL
    # -------------------------------
    grp_est_m = df_long.groupby(["ANIO", "MES", "ESTACION"])["VALOR"]

    dias_totales_est_m = grp_est_m.count()
    prom_m_est = grp_est_m.mean()
    max_diario_est_m = grp_est_m.max()
    dias_cero_est_m = grp_est_m.apply(lambda s: (s == 0).sum())
    dias_exc_est_m = grp_est_m.apply(lambda s: (s > umbral_d).sum())

    pct_cero_est_m = (dias_cero_est_m / dias_totales_est_m.replace(0, np.nan) * 100).round(1)
    pct_exc_est_m = (dias_exc_est_m / dias_totales_est_m.replace(0, np.nan) * 100).round(1)

    modelo_estaciones_mensual = pd.DataFrame({
        "CIUDAD": CIUDAD,
        "CONTAMINANTE": nombre,
        "ANIO": prom_m_est.index.get_level_values("ANIO").astype(int),
        "MES": prom_m_est.index.get_level_values("MES").astype(int),
        "ESTACION": prom_m_est.index.get_level_values("ESTACION"),
        "PERIODO": prom_m_est.index.get_level_values("ANIO").to_series().apply(clasificar_periodo_anual).values,
        "PROM_MENSUAL": prom_m_est.values,
        "MAX_DIARIO_MES": max_diario_est_m.values,
        "DIAS_TOTALES_MES": dias_totales_est_m.values.astype(int),
        "DIAS_CERO_MES": dias_cero_est_m.values.astype(int),
        "PCT_CERO_MES": pct_cero_est_m.values,
        "DIAS_EXC_MES": dias_exc_est_m.values.astype(int),
        "PCT_DIAS_EXC_MES": pct_exc_est_m.values,
        "UMBRAL_DIARIO": umbral_d,
    })

    modelo_estaciones_mensual = modelo_estaciones_mensual[modelo_estaciones_mensual["DIAS_TOTALES_MES"] > 0].reset_index(drop=True)


    # -------------------------------
    # E/F) DIARIO (opcional)
    # -------------------------------
    modelo_ciudad_diario = df_d[["FECHA_DIA", "ANIO", "MES", "PERIODO", "CIUDAD", "ESTACIONES_VALIDAS_DIA"]].copy()
    modelo_ciudad_diario["CIUDAD_NOMBRE"] = CIUDAD
    modelo_ciudad_diario["CONTAMINANTE"] = nombre
    modelo_ciudad_diario = modelo_ciudad_diario.rename(columns={"CIUDAD": "VALOR_DIA"})

    modelo_estacion_diario = df_long[["FECHA_DIA", "ANIO", "MES", "PERIODO", "ESTACION", "VALOR"]].copy()
    modelo_estacion_diario["CIUDAD"] = CIUDAD
    modelo_estacion_diario["CONTAMINANTE"] = nombre

    return (
        modelo_estaciones_anual,
        modelo_ciudad_anual,
        modelo_estaciones_mensual,
        modelo_ciudad_mensual,
        modelo_estacion_diario,
        modelo_ciudad_diario,
    )

# ---------------------------------------------------------------------
# Dimensión de estaciones (shapefile + coords aproximadas)
# ---------------------------------------------------------------------

def construir_dim_estacion(ruta_shapefile: Path, estaciones_modelo: List[str]) -> pd.DataFrame:
    gdf_est = gpd.read_file(ruta_shapefile)

    # Asegurar CRS WGS84 para lat/long
    try:
        if gdf_est.crs is not None and gdf_est.crs.to_epsg() != 4326:
            gdf_est = gdf_est.to_crs(epsg=4326)
    except Exception:
        pass

    # Detectar columna de nombre de estación
    nombre_col = "ESTACION"
    if nombre_col not in gdf_est.columns:
        cand = [c for c in gdf_est.columns if "EST" in str(c).upper()]
        if not cand:
            raise ValueError("No se encontró columna de estación en el shapefile (se esperaba 'ESTACION' o similar).")
        nombre_col = cand[0]

    gdf_est["ESTACION"] = (
        gdf_est[nombre_col]
        .astype(str)
        .apply(normalizar_nombre_estacion)
        .str.upper()
        .str.strip()
    )

    gdf_est["LATITUD"] = gdf_est.geometry.y
    gdf_est["LONGITUD"] = gdf_est.geometry.x

    dim_est_shp = (
        gdf_est
        .groupby("ESTACION")[["LATITUD", "LONGITUD"]]
        .first()
        .reset_index()
    )

    dim_est_shp["CIUDAD"] = CIUDAD
    dim_estacion = dim_est_shp[["CIUDAD", "ESTACION", "LATITUD", "LONGITUD"]].copy()

    for est, (lat, lon) in COORDS_APROX.items():
        est = est.upper()
        mask = dim_estacion["ESTACION"] == est
        if mask.any():
            dim_estacion.loc[mask, "LATITUD"] = dim_estacion.loc[mask, "LATITUD"].fillna(lat)
            dim_estacion.loc[mask, "LONGITUD"] = dim_estacion.loc[mask, "LONGITUD"].fillna(lon)

    estaciones_modelo_u = [normalizar_nombre_estacion(e).upper() for e in estaciones_modelo]
    faltantes = sorted(set(estaciones_modelo_u) - set(dim_estacion["ESTACION"]))
    if faltantes:
        filas = []
        for est in faltantes:
            lat, lon = COORDS_APROX.get(est, (np.nan, np.nan))
            filas.append({"CIUDAD": CIUDAD, "ESTACION": est, "LATITUD": lat, "LONGITUD": lon})
        dim_estacion = pd.concat([dim_estacion, pd.DataFrame(filas)], ignore_index=True)

    dim_estacion = dim_estacion.drop_duplicates(subset=["ESTACION"]).reset_index(drop=True)
    mapping = _generar_codigos_estacion_unicos(dim_estacion["ESTACION"].tolist())
    dim_estacion["COD_ESTACION"] = dim_estacion["ESTACION"].map(mapping).astype(str)

    return dim_estacion[["CIUDAD", "ESTACION", "COD_ESTACION", "LATITUD", "LONGITUD"]].copy()

# ---------------------------------------------------------------------
# Exportación JSONL
# ---------------------------------------------------------------------

def exportar_jsons(
    out_dir: Path,
    modelo_estaciones_anual: pd.DataFrame,
    modelo_ciudad_anual: pd.DataFrame,
    modelo_estaciones_mensual: pd.DataFrame,
    modelo_ciudad_mensual: pd.DataFrame,
    dim_estacion: pd.DataFrame,
    fecha_inicio: Optional[pd.Timestamp],
    fecha_fin: Optional[pd.Timestamp],
    include_diario: bool,
    no_nulls: bool,
    modelo_estacion_diario: Optional[pd.DataFrame] = None,
    modelo_ciudad_diario: Optional[pd.DataFrame] = None,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    for f in out_dir.glob("remmaq_*.json"):
        try:
            f.unlink()
        except OSError:
            pass

    fecha_carga = datetime.utcnow().isoformat()

    path_analitico = out_dir / "remmaq_analitico.json"
    path_geo = out_dir / "remmaq_geo_estaciones.json"

    with path_analitico.open("w", encoding="utf-8") as fa:

        # --------------------
        # 1) CIUDAD - ANUAL
        # --------------------
        for _, row in modelo_ciudad_anual.iterrows():
            anio = _safe_int(row["ANIO"])
            p_start, p_end, e_start, e_end, dias_periodo, dias_eff = _interseccion_periodo(anio, None, fecha_inicio, fecha_fin)
            if dias_eff <= 0:
                continue  # no intersecta la ventana

            dias_totales = max(_safe_int(row.get("DIAS_TOTALES")), 0)
            dias_esperados = dias_eff
            dias_sin_dato = max(dias_esperados - dias_totales, 0)
            pct_sin_dato = round(dias_sin_dato / dias_esperados * 100, 1) if dias_esperados > 0 else (0.0 if no_nulls else None)

            prom = _safe_float(row.get("PROM_ANUAL"), default=None)
            valor_sin_dato = (prom is None) or (dias_totales <= 0)
            tiene_dato = (not valor_sin_dato) and (dias_esperados > 0)

            aplica_umbral = _safe_bool(row.get("APLICA_UMBRAL_ANUAL", False))
            supera = _safe_bool(row.get("SUPERA_OMS_ANUAL", False))

            pct_dias_exc = _emit_pct(row.get("PCT_DIAS_EXC"), no_nulls=no_nulls)
            pct_cero = _emit_pct(row.get("PCT_CERO"), no_nulls=no_nulls)

            estado_anual_oms = _estado_anual_oms(aplica_umbral, supera, tiene_dato)
            estado_diario_oms = _estado_diario_oms(None if valor_sin_dato else row.get("PCT_DIAS_EXC"))
            calidad_dato = _calidad_dato(pct_sin_dato, None if valor_sin_dato else row.get("PCT_CERO"))

            meta = CONFIG_CONTAMINANTES[str(row["CONTAMINANTE"])]

            doc = {
                "schema_version": SCHEMA_VERSION,
                "criterio_calidad_version": CRITERIO_CALIDAD_VERSION,
                "generated_by": GENERATED_BY,

                "id_registro": f"CIUDAD|{row['CONTAMINANTE']}|ANUAL|{anio}",
                "nivel_agregacion": "CIUDAD",
                "nivel_tiempo": "ANUAL",

                "ciudad": row["CIUDAD"],
                "contaminante": row["CONTAMINANTE"],
                "contaminante_nombre": meta["nombre_completo"],
                "unidad": meta["unidad"],

                "anio": anio,
                "mes": (0 if no_nulls else None),
                "anio_mes": (f"{anio:04d}-00" if no_nulls else None),
                "periodo": row["PERIODO"],

                "fecha_inicio_periodo": p_start.date().isoformat(),
                "fecha_fin_periodo": p_end.date().isoformat(),
                "fecha_inicio": e_start.date().isoformat(),
                "fecha_fin": e_end.date().isoformat(),

                "promedio": _emit_float(prom, no_nulls=no_nulls),
                "max_diario": _emit_float(row.get("MAX_DIARIO"), no_nulls=no_nulls),

                "dias_totales": dias_totales,
                "dias_esperados": dias_esperados,
                "dias_sin_dato": dias_sin_dato,
                "pct_sin_dato": _emit_pct(pct_sin_dato, no_nulls=no_nulls),

                "dias_cero": _safe_int(row.get("DIAS_CERO")),
                "pct_cero": pct_cero,

                "dias_exc": _safe_int(row.get("DIAS_EXC")),
                "pct_dias_exc": pct_dias_exc,

                "umbral_diario": _emit_float(row.get("UMBRAL_DIARIO"), no_nulls=no_nulls),
                "umbral_anual": (_emit_float(row.get("UMBRAL_ANUAL"), no_nulls=no_nulls) if aplica_umbral else (0.0 if no_nulls else None)),
                "supera_oms_anual": supera,
                "aplica_umbral_anual": aplica_umbral,

                "estaciones_validas_prom": _emit_float(row.get("ESTACIONES_VALIDAS_PROM"), no_nulls=no_nulls),
                "estaciones_validas_min": _safe_int(row.get("ESTACIONES_VALIDAS_MIN"), default=0),
                "estaciones_validas_max": _safe_int(row.get("ESTACIONES_VALIDAS_MAX"), default=0),

                # Metadatos de regla (útil para auditoría / DAX)
                "metrica_diaria": meta.get("metrica_diaria"),
                "min_horas_dia": _safe_int(meta.get("min_horas_dia"), DEFAULT_MIN_HORAS_DIA),
                "min_estaciones_ciudad": _safe_int(meta.get("min_estaciones_ciudad"), DEFAULT_MIN_ESTACIONES_CIUDAD),

                # Flags BI-safe
                "tiene_dato": bool(tiene_dato),
                "valor_sin_dato": bool(valor_sin_dato),

                "estado_anual_oms": estado_anual_oms,
                "estado_diario_oms": estado_diario_oms,
                "calidad_dato": calidad_dato,
                "fecha_carga": fecha_carga,
            }
            fa.write(json.dumps(doc, ensure_ascii=False, allow_nan=False))
            fa.write("\n")

        # --------------------
        # 2) CIUDAD - MENSUAL
        # --------------------
        for _, row in modelo_ciudad_mensual.iterrows():
            anio = _safe_int(row["ANIO"])
            mes = _safe_int(row["MES"])
            p_start, p_end, e_start, e_end, dias_periodo, dias_eff = _interseccion_periodo(anio, mes, fecha_inicio, fecha_fin)
            if dias_eff <= 0:
                continue

            dias_totales = max(_safe_int(row.get("DIAS_TOTALES_MES")), 0)
            dias_esperados = dias_eff
            dias_sin_dato = max(dias_esperados - dias_totales, 0)
            pct_sin_dato = round(dias_sin_dato / dias_esperados * 100, 1) if dias_esperados > 0 else (0.0 if no_nulls else None)

            prom = _safe_float(row.get("PROM_MENSUAL"), default=None)
            valor_sin_dato = (prom is None) or (dias_totales <= 0)
            tiene_dato = (not valor_sin_dato) and (dias_esperados > 0)

            pct_dias_exc = _emit_pct(row.get("PCT_DIAS_EXC_MES"), no_nulls=no_nulls)
            pct_cero = _emit_pct(row.get("PCT_CERO_MES"), no_nulls=no_nulls)

            meta = CONFIG_CONTAMINANTES[str(row["CONTAMINANTE"])]
            anio_mes = f"{anio:04d}-{mes:02d}"
            trimestre = (mes - 1) // 3 + 1
            semestre = 1 if mes <= 6 else 2

            doc = {
                "schema_version": SCHEMA_VERSION,
                "criterio_calidad_version": CRITERIO_CALIDAD_VERSION,
                "generated_by": GENERATED_BY,

                "id_registro": f"CIUDAD|{row['CONTAMINANTE']}|MENSUAL|{anio}|{mes:02d}",
                "nivel_agregacion": "CIUDAD",
                "nivel_tiempo": "MENSUAL",

                "ciudad": row["CIUDAD"],
                "contaminante": row["CONTAMINANTE"],
                "contaminante_nombre": meta["nombre_completo"],
                "unidad": meta["unidad"],

                "anio": anio,
                "mes": mes,
                "anio_mes": anio_mes,
                "trimestre": trimestre,
                "semestre": semestre,
                "periodo": row["PERIODO"],

                "fecha_inicio_periodo": p_start.date().isoformat(),
                "fecha_fin_periodo": p_end.date().isoformat(),
                "fecha_inicio": e_start.date().isoformat(),
                "fecha_fin": e_end.date().isoformat(),

                "promedio": _emit_float(prom, no_nulls=no_nulls),
                "max_diario": _emit_float(row.get("MAX_DIARIO_MES"), no_nulls=no_nulls),

                "dias_totales": dias_totales,
                "dias_esperados": dias_esperados,
                "dias_sin_dato": dias_sin_dato,
                "pct_sin_dato": _emit_pct(pct_sin_dato, no_nulls=no_nulls),

                "dias_cero": _safe_int(row.get("DIAS_CERO_MES")),
                "pct_cero": pct_cero,

                "dias_exc": _safe_int(row.get("DIAS_EXC_MES")),
                "pct_dias_exc": pct_dias_exc,

                "umbral_diario": _emit_float(row.get("UMBRAL_DIARIO"), no_nulls=no_nulls),
                "umbral_anual": (0.0 if no_nulls else None),
                "supera_oms_anual": False,
                "aplica_umbral_anual": False,

                "estaciones_validas_prom": _emit_float(row.get("ESTACIONES_VALIDAS_PROM"), no_nulls=no_nulls),
                "estaciones_validas_min": _safe_int(row.get("ESTACIONES_VALIDAS_MIN"), default=0),
                "estaciones_validas_max": _safe_int(row.get("ESTACIONES_VALIDAS_MAX"), default=0),

                "metrica_diaria": meta.get("metrica_diaria"),
                "min_horas_dia": _safe_int(meta.get("min_horas_dia"), DEFAULT_MIN_HORAS_DIA),
                "min_estaciones_ciudad": _safe_int(meta.get("min_estaciones_ciudad"), DEFAULT_MIN_ESTACIONES_CIUDAD),

                "tiene_dato": bool(tiene_dato),
                "valor_sin_dato": bool(valor_sin_dato),

                "estado_anual_oms": "SIN_UMBRAL",
                "estado_diario_oms": _estado_diario_oms(None if valor_sin_dato else row.get("PCT_DIAS_EXC_MES")),
                "calidad_dato": _calidad_dato(pct_sin_dato, None if valor_sin_dato else row.get("PCT_CERO_MES")),
                "fecha_carga": fecha_carga,
            }
            fa.write(json.dumps(doc, ensure_ascii=False, allow_nan=False))
            fa.write("\n")

        # --------------------
        # 3) ESTACIONES - ANUAL
        # --------------------
        est_anual_geo = modelo_estaciones_anual.merge(
            dim_estacion[["ESTACION", "COD_ESTACION", "LATITUD", "LONGITUD"]],
            on="ESTACION",
            how="left",
        )

        for _, row in est_anual_geo.iterrows():
            anio = _safe_int(row["ANIO"])
            p_start, p_end, e_start, e_end, dias_periodo, dias_eff = _interseccion_periodo(anio, None, fecha_inicio, fecha_fin)
            if dias_eff <= 0:
                continue

            dias_totales = max(_safe_int(row.get("DIAS_TOTALES")), 0)
            dias_esperados = dias_eff
            dias_sin_dato = max(dias_esperados - dias_totales, 0)
            pct_sin_dato = round(dias_sin_dato / dias_esperados * 100, 1) if dias_esperados > 0 else (0.0 if no_nulls else None)

            prom = _safe_float(row.get("PROM_ANUAL"), default=None)
            valor_sin_dato = (prom is None) or (dias_totales <= 0)
            tiene_dato = (not valor_sin_dato) and (dias_esperados > 0)

            aplica_umbral = _safe_bool(row.get("APLICA_UMBRAL_ANUAL", False))
            supera = _safe_bool(row.get("SUPERA_OMS_ANUAL", False))

            pct_dias_exc = _emit_pct(row.get("PCT_DIAS_EXC"), no_nulls=no_nulls)
            pct_cero = _emit_pct(row.get("PCT_CERO"), no_nulls=no_nulls)

            meta = CONFIG_CONTAMINANTES[str(row["CONTAMINANTE"])]
            estacion = str(row["ESTACION"]).upper()
            id_estacion = str(row.get("COD_ESTACION") or "").upper()

            lat_raw = _safe_float(row.get("LATITUD"), default=None)
            lon_raw = _safe_float(row.get("LONGITUD"), default=None)
            tiene_geoloc = (lat_raw is not None) and (lon_raw is not None)

            doc = {
                "schema_version": SCHEMA_VERSION,
                "criterio_calidad_version": CRITERIO_CALIDAD_VERSION,
                "generated_by": GENERATED_BY,

                "id_registro": f"ESTACION|{id_estacion}|{row['CONTAMINANTE']}|ANUAL|{anio}",
                "nivel_agregacion": "ESTACION",
                "nivel_tiempo": "ANUAL",

                "ciudad": row["CIUDAD"],
                "contaminante": row["CONTAMINANTE"],
                "contaminante_nombre": meta["nombre_completo"],
                "unidad": meta["unidad"],

                "estacion": estacion,
                "id_estacion": id_estacion,

                "anio": anio,
                "mes": (0 if no_nulls else None),
                "anio_mes": (f"{anio:04d}-00" if no_nulls else None),
                "periodo": row["PERIODO"],

                "fecha_inicio_periodo": p_start.date().isoformat(),
                "fecha_fin_periodo": p_end.date().isoformat(),
                "fecha_inicio": e_start.date().isoformat(),
                "fecha_fin": e_end.date().isoformat(),

                "latitud": (lat_raw if tiene_geoloc else (SENTINEL_GEO if no_nulls else None)),
                "longitud": (lon_raw if tiene_geoloc else (SENTINEL_GEO if no_nulls else None)),
                "tiene_geoloc": bool(tiene_geoloc),

                "promedio": _emit_float(prom, no_nulls=no_nulls),
                "max_diario": _emit_float(row.get("MAX_DIARIO"), no_nulls=no_nulls),

                "dias_totales": dias_totales,
                "dias_esperados": dias_esperados,
                "dias_sin_dato": dias_sin_dato,
                "pct_sin_dato": _emit_pct(pct_sin_dato, no_nulls=no_nulls),

                "dias_cero": _safe_int(row.get("DIAS_CERO")),
                "pct_cero": pct_cero,

                "dias_exc": _safe_int(row.get("DIAS_EXC")),
                "pct_dias_exc": pct_dias_exc,

                "umbral_diario": _emit_float(row.get("UMBRAL_DIARIO"), no_nulls=no_nulls),
                "umbral_anual": (_emit_float(row.get("UMBRAL_ANUAL"), no_nulls=no_nulls) if aplica_umbral else (0.0 if no_nulls else None)),
                "supera_oms_anual": supera,
                "aplica_umbral_anual": aplica_umbral,

                "metrica_diaria": meta.get("metrica_diaria"),
                "min_horas_dia": _safe_int(meta.get("min_horas_dia"), DEFAULT_MIN_HORAS_DIA),

                "tiene_dato": bool(tiene_dato),
                "valor_sin_dato": bool(valor_sin_dato),

                "estado_anual_oms": _estado_anual_oms(aplica_umbral, supera, tiene_dato),
                "estado_diario_oms": _estado_diario_oms(None if valor_sin_dato else row.get("PCT_DIAS_EXC")),
                "calidad_dato": _calidad_dato(pct_sin_dato, None if valor_sin_dato else row.get("PCT_CERO")),
                "fecha_carga": fecha_carga,
            }
            fa.write(json.dumps(doc, ensure_ascii=False, allow_nan=False))
            fa.write("\n")

        # --------------------
        # 4) ESTACIONES - MENSUAL
        # --------------------
        est_mensual_geo = modelo_estaciones_mensual.merge(
            dim_estacion[["ESTACION", "COD_ESTACION", "LATITUD", "LONGITUD"]],
            on="ESTACION",
            how="left",
        )

        for _, row in est_mensual_geo.iterrows():
            anio = _safe_int(row["ANIO"])
            mes = _safe_int(row["MES"])
            p_start, p_end, e_start, e_end, dias_periodo, dias_eff = _interseccion_periodo(anio, mes, fecha_inicio, fecha_fin)
            if dias_eff <= 0:
                continue

            dias_totales = max(_safe_int(row.get("DIAS_TOTALES_MES")), 0)
            dias_esperados = dias_eff
            dias_sin_dato = max(dias_esperados - dias_totales, 0)
            pct_sin_dato = round(dias_sin_dato / dias_esperados * 100, 1) if dias_esperados > 0 else (0.0 if no_nulls else None)

            prom = _safe_float(row.get("PROM_MENSUAL"), default=None)
            valor_sin_dato = (prom is None) or (dias_totales <= 0)
            tiene_dato = (not valor_sin_dato) and (dias_esperados > 0)

            pct_dias_exc = _emit_pct(row.get("PCT_DIAS_EXC_MES"), no_nulls=no_nulls)
            pct_cero = _emit_pct(row.get("PCT_CERO_MES"), no_nulls=no_nulls)

            meta = CONFIG_CONTAMINANTES[str(row["CONTAMINANTE"])]
            estacion = str(row["ESTACION"]).upper()
            id_estacion = str(row.get("COD_ESTACION") or "").upper()

            lat_raw = _safe_float(row.get("LATITUD"), default=None)
            lon_raw = _safe_float(row.get("LONGITUD"), default=None)
            tiene_geoloc = (lat_raw is not None) and (lon_raw is not None)

            anio_mes = f"{anio:04d}-{mes:02d}"
            trimestre = (mes - 1) // 3 + 1
            semestre = 1 if mes <= 6 else 2

            doc = {
                "schema_version": SCHEMA_VERSION,
                "criterio_calidad_version": CRITERIO_CALIDAD_VERSION,
                "generated_by": GENERATED_BY,

                "id_registro": f"ESTACION|{id_estacion}|{row['CONTAMINANTE']}|MENSUAL|{anio}|{mes:02d}",
                "nivel_agregacion": "ESTACION",
                "nivel_tiempo": "MENSUAL",

                "ciudad": row["CIUDAD"],
                "contaminante": row["CONTAMINANTE"],
                "contaminante_nombre": meta["nombre_completo"],
                "unidad": meta["unidad"],

                "estacion": estacion,
                "id_estacion": id_estacion,

                "anio": anio,
                "mes": mes,
                "anio_mes": anio_mes,
                "trimestre": trimestre,
                "semestre": semestre,
                "periodo": row["PERIODO"],

                "fecha_inicio_periodo": p_start.date().isoformat(),
                "fecha_fin_periodo": p_end.date().isoformat(),
                "fecha_inicio": e_start.date().isoformat(),
                "fecha_fin": e_end.date().isoformat(),

                "latitud": (lat_raw if tiene_geoloc else (SENTINEL_GEO if no_nulls else None)),
                "longitud": (lon_raw if tiene_geoloc else (SENTINEL_GEO if no_nulls else None)),
                "tiene_geoloc": bool(tiene_geoloc),

                "promedio": _emit_float(prom, no_nulls=no_nulls),
                "max_diario": _emit_float(row.get("MAX_DIARIO_MES"), no_nulls=no_nulls),

                "dias_totales": dias_totales,
                "dias_esperados": dias_esperados,
                "dias_sin_dato": dias_sin_dato,
                "pct_sin_dato": _emit_pct(pct_sin_dato, no_nulls=no_nulls),

                "dias_cero": _safe_int(row.get("DIAS_CERO_MES")),
                "pct_cero": pct_cero,

                "dias_exc": _safe_int(row.get("DIAS_EXC_MES")),
                "pct_dias_exc": pct_dias_exc,

                "umbral_diario": _emit_float(row.get("UMBRAL_DIARIO"), no_nulls=no_nulls),
                "umbral_anual": (0.0 if no_nulls else None),
                "supera_oms_anual": False,
                "aplica_umbral_anual": False,

                "metrica_diaria": meta.get("metrica_diaria"),
                "min_horas_dia": _safe_int(meta.get("min_horas_dia"), DEFAULT_MIN_HORAS_DIA),

                "tiene_dato": bool(tiene_dato),
                "valor_sin_dato": bool(valor_sin_dato),

                "estado_anual_oms": "SIN_UMBRAL",
                "estado_diario_oms": _estado_diario_oms(None if valor_sin_dato else row.get("PCT_DIAS_EXC_MES")),
                "calidad_dato": _calidad_dato(pct_sin_dato, None if valor_sin_dato else row.get("PCT_CERO_MES")),
                "fecha_carga": fecha_carga,
            }
            fa.write(json.dumps(doc, ensure_ascii=False, allow_nan=False))
            fa.write("\n")

        # --------------------
        # 5) (Opcional) DIARIO
        # --------------------
        if include_diario and (modelo_ciudad_diario is not None) and (modelo_estacion_diario is not None):
            for _, row in modelo_ciudad_diario.iterrows():
                fecha = pd.to_datetime(row["FECHA_DIA"]).date().isoformat()
                anio = _safe_int(row["ANIO"])
                mes = _safe_int(row["MES"])
                meta = CONFIG_CONTAMINANTES[str(row["CONTAMINANTE"])]

                v = _safe_float(row.get("VALOR_DIA"), default=None)
                valor_sin_dato = v is None
                doc = {
                    "schema_version": SCHEMA_VERSION,
                    "criterio_calidad_version": CRITERIO_CALIDAD_VERSION,
                    "generated_by": GENERATED_BY,

                    "id_registro": f"CIUDAD|{row['CONTAMINANTE']}|DIARIO|{fecha}",
                    "nivel_agregacion": "CIUDAD",
                    "nivel_tiempo": "DIARIO",

                    "ciudad": CIUDAD,
                    "contaminante": row["CONTAMINANTE"],
                    "contaminante_nombre": meta["nombre_completo"],
                    "unidad": meta["unidad"],

                    "anio": anio,
                    "mes": mes,
                    "anio_mes": f"{anio:04d}-{mes:02d}",
                    "fecha": fecha,

                    "valor": _emit_float(v, no_nulls=no_nulls),
                    "estaciones_validas_dia": _safe_int(row.get("ESTACIONES_VALIDAS_DIA"), default=0),
                    "umbral_diario": _emit_float(meta.get("umbral_diario"), no_nulls=no_nulls),
                    "metrica_diaria": meta.get("metrica_diaria"),
                    "min_horas_dia": _safe_int(meta.get("min_horas_dia"), DEFAULT_MIN_HORAS_DIA),
                    "min_estaciones_ciudad": _safe_int(meta.get("min_estaciones_ciudad"), DEFAULT_MIN_ESTACIONES_CIUDAD),

                    "tiene_dato": not valor_sin_dato,
                    "valor_sin_dato": bool(valor_sin_dato),

                    "fecha_carga": fecha_carga,
                }
                fa.write(json.dumps(doc, ensure_ascii=False, allow_nan=False))
                fa.write("\n")

            diario_est_geo = modelo_estacion_diario.merge(
                dim_estacion[["ESTACION", "COD_ESTACION", "LATITUD", "LONGITUD"]],
                on="ESTACION",
                how="left",
            )

            for _, row in diario_est_geo.iterrows():
                fecha = pd.to_datetime(row["FECHA_DIA"]).date().isoformat()
                anio = _safe_int(row["ANIO"])
                mes = _safe_int(row["MES"])
                meta = CONFIG_CONTAMINANTES[str(row["CONTAMINANTE"])]

                estacion = str(row["ESTACION"]).upper()
                id_estacion = str(row.get("COD_ESTACION") or "").upper()

                lat_raw = _safe_float(row.get("LATITUD"), default=None)
                lon_raw = _safe_float(row.get("LONGITUD"), default=None)
                tiene_geoloc = (lat_raw is not None) and (lon_raw is not None)

                v = _safe_float(row.get("VALOR"), default=None)
                valor_sin_dato = v is None

                doc = {
                    "schema_version": SCHEMA_VERSION,
                    "criterio_calidad_version": CRITERIO_CALIDAD_VERSION,
                    "generated_by": GENERATED_BY,

                    "id_registro": f"ESTACION|{id_estacion}|{row['CONTAMINANTE']}|DIARIO|{fecha}",
                    "nivel_agregacion": "ESTACION",
                    "nivel_tiempo": "DIARIO",

                    "ciudad": CIUDAD,
                    "contaminante": row["CONTAMINANTE"],
                    "contaminante_nombre": meta["nombre_completo"],
                    "unidad": meta["unidad"],

                    "estacion": estacion,
                    "id_estacion": id_estacion,

                    "anio": anio,
                    "mes": mes,
                    "anio_mes": f"{anio:04d}-{mes:02d}",
                    "fecha": fecha,

                    "latitud": (lat_raw if tiene_geoloc else (SENTINEL_GEO if no_nulls else None)),
                    "longitud": (lon_raw if tiene_geoloc else (SENTINEL_GEO if no_nulls else None)),
                    "tiene_geoloc": bool(tiene_geoloc),

                    "valor": _emit_float(v, no_nulls=no_nulls),
                    "umbral_diario": _emit_float(meta.get("umbral_diario"), no_nulls=no_nulls),
                    "metrica_diaria": meta.get("metrica_diaria"),
                    "min_horas_dia": _safe_int(meta.get("min_horas_dia"), DEFAULT_MIN_HORAS_DIA),

                    "tiene_dato": not valor_sin_dato,
                    "valor_sin_dato": bool(valor_sin_dato),

                    "fecha_carga": fecha_carga,
                }
                fa.write(json.dumps(doc, ensure_ascii=False, allow_nan=False))
                fa.write("\n")

    # GEO estaciones (dimensión)
    with path_geo.open("w", encoding="utf-8") as fg:
        for _, row in dim_estacion.iterrows():
            estacion = str(row["ESTACION"]).upper()
            id_estacion = str(row["COD_ESTACION"]).upper()

            lat_raw = _safe_float(row.get("LATITUD"), default=None)
            lon_raw = _safe_float(row.get("LONGITUD"), default=None)
            tiene_geoloc = (lat_raw is not None) and (lon_raw is not None)

            doc_geo = {
                "schema_version": SCHEMA_VERSION,
                "generated_by": GENERATED_BY,
                "id_estacion": id_estacion,
                "ciudad": row["CIUDAD"],
                "estacion": estacion,
                "cod_estacion": id_estacion,
                "latitud": (lat_raw if tiene_geoloc else (SENTINEL_GEO if no_nulls else None)),
                "longitud": (lon_raw if tiene_geoloc else (SENTINEL_GEO if no_nulls else None)),
                "tiene_geoloc": bool(tiene_geoloc),
                "fecha_carga": fecha_carga,
            }
            fg.write(json.dumps(doc_geo, ensure_ascii=False, allow_nan=False))
            fg.write("\n")

    print(f"[INFO] Generados: {path_analitico.name} y {path_geo.name} en {out_dir}", file=sys.stderr)


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="REMMAQ - Generador de modelo analítico para MongoDB")
    parser.add_argument("--in_dir", required=True, help="Directorio de entrada con archivos de contaminantes (.xlsx)")
    parser.add_argument("--out_dir", required=True, help="Directorio de salida para JSON generados")
    parser.add_argument("--shapefile", required=True, help="Ruta al shapefile de estaciones REMMAQ")
    parser.add_argument("--start", type=str, default=None, help="Fecha inicio (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="Fecha fin (YYYY-MM-DD)")
    parser.add_argument("--include_diario", action="store_true", help="Incluye registros DIARIO (aumenta tamaño del JSON)")
    parser.add_argument("--no_nulls", action="store_true", help="Modo BI-safe: evita NULLs en métricas numéricas y agrega flags.")
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    shp_path = Path(args.shapefile)

    fecha_inicio = pd.to_datetime(args.start) if args.start else None
    fecha_fin = _normalizar_fecha_fin_inclusiva(args.end)

    modelos_est_anual = []
    modelos_ci_anual = []
    modelos_est_mensual = []
    modelos_ci_mensual = []
    diarios_est = []
    diarios_ci = []
    estaciones_en_modelo = set()

    for nombre, archivo in ARCHIVOS_POR_CONTAMINANTE.items():
        ruta_arch = in_dir / archivo
        if not ruta_arch.exists():
            print(f"[WARN] No se encontró archivo para {nombre}: {ruta_arch}", file=sys.stderr)
            continue

        print(f"[INFO] Procesando contaminante {nombre} desde {ruta_arch} ...", file=sys.stderr)

        df_horas = leer_horario_xlsx(ruta_arch, contaminante=nombre)

        # QA: resumen horario por estación (match 1:1 con Excel)
        n_rows = len(df_horas)
        qa_horario = {
            "contaminante": nombre,
            "n_rows": n_rows,
            "estaciones": {
                c: {
                    "media_horaria": float(df_horas[c].mean(skipna=True)) if n_rows else None,
                    "cobertura_horaria": float(df_horas[c].count() / n_rows) if n_rows else None,
                }
                for c in [x for x in df_horas.columns if x != "FECHA_HORA"]
            }
        }


        df_diario, estaciones_cols = construir_df_diario(df_horas, nombre, fecha_inicio, fecha_fin)

        estaciones_en_modelo.update(estaciones_cols)

        mod_est_a, mod_ci_a, mod_est_m, mod_ci_m, mod_est_d, mod_ci_d = modelos_para_contaminante(nombre, df_diario, estaciones_cols)

        modelos_est_anual.append(mod_est_a)
        modelos_ci_anual.append(mod_ci_a)
        modelos_est_mensual.append(mod_est_m)
        modelos_ci_mensual.append(mod_ci_m)

        if args.include_diario:
            diarios_est.append(mod_est_d)
            diarios_ci.append(mod_ci_d)

    if not modelos_est_anual:
        raise RuntimeError("No se generó información para ningún contaminante (verificar archivos de entrada).")

    modelo_estaciones_anual = pd.concat(modelos_est_anual, ignore_index=True)
    modelo_ciudad_anual = pd.concat(modelos_ci_anual, ignore_index=True)
    modelo_estaciones_mensual = pd.concat(modelos_est_mensual, ignore_index=True)
    modelo_ciudad_mensual = pd.concat(modelos_ci_mensual, ignore_index=True)

    modelo_estacion_diario = pd.concat(diarios_est, ignore_index=True) if diarios_est else None
    modelo_ciudad_diario = pd.concat(diarios_ci, ignore_index=True) if diarios_ci else None

    dim_estacion = construir_dim_estacion(shp_path, list(estaciones_en_modelo))

    exportar_jsons(
        out_dir=out_dir,
        modelo_estaciones_anual=modelo_estaciones_anual,
        modelo_ciudad_anual=modelo_ciudad_anual,
        modelo_estaciones_mensual=modelo_estaciones_mensual,
        modelo_ciudad_mensual=modelo_ciudad_mensual,
        dim_estacion=dim_estacion,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        include_diario=args.include_diario,
        no_nulls=args.no_nulls,
        modelo_estacion_diario=modelo_estacion_diario,
        modelo_ciudad_diario=modelo_ciudad_diario,
    )

    resultado = {
        "estado": "OK",
        "schema_version": SCHEMA_VERSION,
        "mensaje": "JSON generados correctamente",
        "ruta_salida": str(out_dir),
        "timestamp": datetime.utcnow().isoformat(),
        "include_diario": bool(args.include_diario),
        "no_nulls": bool(args.no_nulls),
    }
    print(json.dumps(resultado, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
