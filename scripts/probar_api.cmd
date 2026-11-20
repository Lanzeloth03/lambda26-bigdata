@echo off
REM Prueba la API real de Open-Meteo desde el contenedor. Uso: scripts\probar_api.cmd [publicar]
cd /d "%~dp0.."
docker cp s12-aire\scripts\probar_api.py lambda26-pyspark:/tmp/s12/probar_api.py
if "%1"=="publicar" (docker exec lambda26-pyspark python /tmp/s12/probar_api.py --publicar > scripts\_api.txt 2>&1) else (docker exec lambda26-pyspark python /tmp/s12/probar_api.py > scripts\_api.txt 2>&1)
type scripts\_api.txt
