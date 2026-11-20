@echo off
REM Muestra el estado del demo en vivo y lo guarda en scripts\_estado.txt
cd /d "%~dp0.."
docker cp s12-aire\scripts\estado_demo.py lambda26-pyspark:/tmp/s12/scripts/estado_demo.py
docker exec lambda26-pyspark python /tmp/s12/scripts/estado_demo.py > scripts\_estado.txt 2>&1
docker ps --format "{{.Names}} {{.Status}}" >> scripts\_estado.txt
type scripts\_estado.txt
