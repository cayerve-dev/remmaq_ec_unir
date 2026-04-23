# --- Dockerfile for NiFi + Python ETL (REMMAQ) ---
FROM apache/nifi:1.25.0

USER root
RUN set -eux; apt-get update; apt-get install -y --no-install-recommends python3 python3-pip ca-certificates jq libarchive-tools unar unrar; apt-get install -y --no-install-recommends 7zip || true; apt-get install -y --no-install-recommends p7zip-full || true; if command -v 7zz >/dev/null 2>&1 && [ ! -e /usr/bin/7z ]; then ln -s "$(command -v 7zz)" /usr/bin/7z; fi; rm -rf /var/lib/apt/lists/*
# Python deps (mínimos requeridos por el script)
RUN pip3 install --no-cache-dir numpy pandas openpyxl geopandas pymongo requests beautifulsoup4 rarfile
# Estructura de trabajo y permisos
RUN mkdir -p /opt/etl/scripts /opt/etl/data /opt/etl/logs /opt/etl/geoportal && chown -R nifi:nifi /opt/etl
USER nifi
ENV NIFI_WEB_HTTP_PORT=8080