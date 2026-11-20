@echo off
REM Reinicio LIMPIO del demo continuo (Grafana vuelve a empezar desde cero, sin duplicados ni descartes espurios).
REM  1) mata alimentadores y job  2) borra y recrea atmos-aire-demo  3) limpia metricas del Pushgateway  4) arranca el job con --limpiar
REM  5) alimenta el periodo de TEST de U1 (2023-10-20 en adelante, 1 evento cada 10 s) y despues la API cada hora.
REM No toca atmos-aire, atmos-aire-hist-* ni atmos-negocio.  Uso: scripts\reiniciar_demo.cmd [PAUSA_SEGUNDOS]
cd /d "%~dp0.."
set PAUSA=%1
if "%PAUSA%"=="" set PAUSA=10
echo [1/5] Deteniendo procesos anteriores...
docker cp scripts\detener_todo.py lambda26-pyspark:/tmp/s12/detener_todo.py
docker exec lambda26-pyspark python /tmp/s12/detener_todo.py
echo [2/5] Recreando topic atmos-aire-demo...
docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --delete --topic atmos-aire-demo --bootstrap-server kafka:9092
timeout /t 5 /nobreak >nul
docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic atmos-aire-demo --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
echo [3/5] Limpiando metricas viejas del Pushgateway...
docker exec lambda26-pyspark python -c "import urllib.request as u;u.urlopen(u.Request('http://pushgateway:9091/metrics/job/lambda26_aire',method='DELETE'));print('metricas borradas')"
echo [4/5] Job de streaming...
docker exec -d lambda26-pyspark sh -c "echo $$ > /tmp/s12/job.pid; exec spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /tmp/s12/scripts/s12_streaming_aire.py --modelo /tmp/s12/modelo_pm25 --topic atmos-aire-demo --limpiar > /tmp/s12/job_aire.log 2>&1"
timeout /t 45 /nobreak >nul
echo [5/5] Alimentador (1 evento cada %PAUSA% s, desde 2023-10-20)...
docker exec -d lambda26-pyspark sh -c "echo $$ > /tmp/s12/alim.pid; python /tmp/s12/scripts/openmeteo_a_kafka.py --archivo /tmp/s12/aq_juliaca_2023.json --desde 2023-10-20T00:00 --pausa %PAUSA% --topic atmos-aire-demo; while true; do python /tmp/s12/scripts/openmeteo_a_kafka.py --api --topic atmos-aire-demo --desde $(date -u -d '2 hours ago' +%%Y-%%m-%%dT%%H:00); sleep 3600; done > /tmp/s12/alimentador.log 2>&1"
echo.
echo LISTO. Grafana: http://localhost:43000 (tablero S12). Los paneles se llenan solos; el replay dura unas 4,9 h a 10 s por evento.
