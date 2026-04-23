# 🌫️ REMMAQ — Gestión Analítica de Episodios Contaminantes en Quito, Ecuador

> **ETL multitemporal, KPIs y Visual Analytics sobre datos de calidad del aire (2015–2025)**

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-Docker-728E9B?style=flat&logo=apache&logoColor=white)](https://nifi.apache.org/)
[![MongoDB Atlas](https://img.shields.io/badge/MongoDB-Atlas-47A248?style=flat&logo=mongodb&logoColor=white)](https://www.mongodb.com/atlas)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📋 Descripción

Este repositorio contiene el **protocolo ETL reproducible** desarrollado como Trabajo Fin de Máster en el programa de *Análisis y Visualización de Datos Masivos / Visual Analytics and Big Data* de la **Universidad Internacional de La Rioja (UNIR)**.

El proyecto diseña y valida un flujo de ingeniería de datos para la **Red Metropolitana de Monitoreo Atmosférico de Quito (REMMAQ)**, transformando publicaciones mensuales en Excel en inteligencia operativa sobre episodios de contaminación atmosférica.

**Período de análisis:** 2015 – 2025  
**Área de estudio:** Distrito Metropolitano de Quito, Ecuador  
**Contaminantes analizados:** CO, NO₂, O₃, PM2.5, PM10, SO₂  
**Estaciones monitoreadas:** 9 estaciones de la red REMMAQ

---

## 🎯 Objetivos

- Diseñar un protocolo ETL mensual **trazable y parametrizable** en Python
- Detectar episodios contaminantes aplicando **valores guía OMS 2021** y criterios de persistencia
- Consolidar el histórico 2015–2025 en **MongoDB Atlas** para consulta y análisis
- Calcular **KPIs de superación**, frecuencia, duración y tendencias por estación y contaminante
- Presentar resultados en un **dashboard con semaforización** en Power BI

---

## 🏗️ Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────┐
│                        FUENTE DE DATOS                          │
│          REMMAQ — Publicaciones mensuales Excel (.xlsx)         │
│          (por contaminante y estación, formato ancho)           │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    EXTRACCIÓN (E)                                │
│              Apache NiFi orquestado en Docker                   │
│         Descarga automatizada de paquetes por período           │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                  TRANSFORMACIÓN (T)                             │
│                    Python (pandas)                              │
│  • Ancho → Largo (melt)          • Control de calidad (QA/QC)  │
│  • Normalización de unidades     • Validación de rangos         │
│  • Corrección de zona horaria    • Detección de duplicados      │
│  • Enriquecimiento geográfico    • Marcado de nulos/ceros       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CARGA (L)                                  │
│                    MongoDB Atlas                                │
│         Atlas Data Federation — RemmaqDatabaseFederation        │
│              Trazabilidad por período y contaminante            │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│               ANÁLISIS Y VISUALIZACIÓN                          │
│                Power BI — Modelo estrella                       │
│    DAX Measures · KPIs · Semaforización · Dashboard operativo   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🗂️ Estructura del Repositorio

```
REMMAQ/
└── ETL/
    ├── dashboard/              # Configuración del dashboard Power BI
    │   └── theme_quito_aire.json
    ├── data/
    │   └── remmaq/
    │       ├── extraidos/      # Datos procesados por contaminante (muestra)
    │       │   └── CO_2.xlsx   # Archivo de muestra incluido
    │       ├── normalizados/   # Series transformadas a formato largo
    │       └── originales/     # Datos fuente REMMAQ (excluidos por tamaño)
    ├── docker/
    │   └── nifi/
    │       └── Dockerfile      # Imagen NiFi personalizada
    ├── geoportal/              # Shapefiles de estaciones REMMAQ (9 estaciones)
    │   └── DC002_ESTACION_CALIDAD_AIRE_P.*
    ├── logs/                   # Registros de ejecución ETL
    │   ├── load_resumen.json
    │   └── transformacion_resumen.json
    ├── notebooks/
    │   └── ANALISIS_EDA_REMMAQ.ipynb   # Análisis Exploratorio completo
    ├── scripts/
    │   ├── ETL_REMMAQ_EXTRACCION.py    # Módulo de extracción (NiFi)
    │   ├── ETL_REMMAQ_TRANSFORMACION.py # Módulo de transformación
    │   └── ETL_REMMAQ_LOAD.py          # Módulo de carga a MongoDB
    ├── docker-compose.yml      # Orquestación Docker (NiFi + dependencias)
    └── comandos-docker.txt     # Referencia de comandos operativos
```

---

## 🛠️ Stack Tecnológico

| Capa | Tecnología | Versión |
|------|-----------|---------|
| Lenguaje principal | Python | 3.10+ |
| Orquestación ETL | Apache NiFi | Docker |
| Contenedores | Docker / Docker Compose | — |
| Base de datos | MongoDB Atlas | — |
| Federación de datos | Atlas Data Federation | — |
| Visualización | Power BI Desktop | — |
| Análisis exploratorio | Jupyter Notebook | — |
| Datos geoespaciales | Shapefiles (QGIS) | — |

**Dependencias Python principales:**
```
pandas · pymongo · openpyxl · geopandas · python-dotenv
```

---

## ⚙️ Configuración y Ejecución

### 1. Prerequisitos

- Docker Desktop instalado y en ejecución
- Python 3.10+
- Cuenta en MongoDB Atlas
- Credenciales configuradas en variables de entorno (`.env`)

### 2. Variables de entorno

Crea un archivo `.env` en la raíz del proyecto (nunca lo subas a Git):

```env
MONGO_URI=mongodb+srv://<usuario>:<password>@<cluster>.mongodb.net/
MONGO_DB=remmaq
MONGO_COLLECTION=mediciones
```

### 3. Levantar Apache NiFi con Docker

```bash
cd ETL/
docker-compose up -d
```

NiFi estará disponible en: `https://localhost:8443/nifi`

### 4. Ejecutar el pipeline ETL manualmente

```bash
# Extracción
python ETL/scripts/ETL_REMMAQ_EXTRACCION.py

# Transformación
python ETL/scripts/ETL_REMMAQ_TRANSFORMACION.py

# Carga a MongoDB Atlas
python ETL/scripts/ETL_REMMAQ_LOAD.py
```

### 5. Análisis exploratorio

```bash
jupyter notebook ETL/notebooks/ANALISIS_EDA_REMMAQ.ipynb
```

---

## 📊 Resultados Destacados

| Indicador | Resultado |
|-----------|-----------|
| Período analizado | 2015 – 2025 |
| Estaciones procesadas | 9 estaciones REMMAQ |
| Contaminantes | CO, NO₂, O₃, PM2.5, PM10, SO₂ |
| Días con excedencia PM2.5 | **49.09%** del período analizado |
| Efecto COVID-19 | Transición detectada en 3 ventanas temporales |
| Norma de referencia | OMS 2021 (valores guía exposición corta duración) |

> El contaminante **PM2.5** presenta el mayor número de días con superación de valores guía OMS, con patrón estacional marcado. Se detecta un efecto transitorio asociado al período COVID-19 (2020–2021) en la mayoría de estaciones.

---

## 🗺️ Estaciones REMMAQ

Las 9 estaciones de monitoreo del Distrito Metropolitano de Quito cubren distintas zonas urbanas. Los shapefiles con ubicación y atributos están disponibles en `ETL/geoportal/`.

| Zona | Cobertura |
|------|-----------|
| Norte | Estaciones de zonas residenciales y comerciales |
| Centro | Zona histórica y alta densidad vehicular |
| Sur | Zonas industriales y periféricas |
| Valles | Tumbaco, Los Chillos |

---

## 📐 Modelo de Datos (Power BI)

El modelo semántico en Power BI implementa un **esquema estrella** con:

- **Tabla de hechos:** mediciones horarias por estación y contaminante
- **Dimensión tiempo:** año, mes, semana, día, hora
- **Dimensión estación:** nombre, zona, coordenadas
- **Dimensión contaminante:** nombre, unidad, umbral OMS
- **Medidas DAX:** días de excedencia, episodios, promedios móviles, índice de calidad

---

## 👥 Autores

| Nombre | Rol |
|--------|-----|
| **Cesar Patricio Ayerve Ramos** | Arquitectura ETL, ingeniería de datos, MongoDB, KPIs |
| **Joselin Yadira Lema Ushiña** | Visualización, dashboard Power BI, storytelling |

**Director:** Alexandre Pérez Reina  
**Institución:** Universidad Internacional de La Rioja (UNIR)  
**Programa:** Máster Universitario en Análisis y Visualización de Datos Masivos  
**Año:** 2026

---

## 📄 Cita

Si utilizas este trabajo en tu investigación:

```bibtex
@mastersthesis{ayerve2026remmaq,
  author    = {Ayerve Ramos, Cesar Patricio and Lema Ushi{\~n}a, Joselin Yadira},
  title     = {Gesti{\'o}n anal{\'i}tica de episodios contaminantes en Quito, Ecuador (REMMAQ):
               ETL multitemporal, KPIs y visual analytics},
  school    = {Universidad Internacional de La Rioja (UNIR)},
  year      = {2026},
  type      = {Trabajo Fin de M{\'a}ster}
}
```

---

## 📜 Licencia

Este proyecto está bajo la licencia [MIT](LICENSE). Los datos utilizados son de acceso público, publicados por el Municipio del Distrito Metropolitano de Quito a través de la REMMAQ.

---

<div align="center">
  <sub>Desarrollado en Quito, Ecuador · UNIR 2026</sub>
</div>