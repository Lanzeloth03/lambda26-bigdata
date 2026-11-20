@echo off
REM Mide la latencia Kafka -> Grafana en el demo. SOLO LECTURA: no publica, no borra, no reinicia nada.
cd /d "%~dp0.."
docker cp s12-aire\scripts\latencia_demo.py lambda26-pyspark:/tmp/s12/latencia_demo.py
docker exec lambda26-pyspark python -W ignore /tmp/s12/latencia_demo.py %1 > scripts\_latencia.txt 2>&1
type scripts\_latencia.txt
