@echo off
REM Reinicio LIMPIO del demo en modo REAL: Grafana se alimenta con los datos reales de la API publica Open-Meteo (CAMS).
REM  1) mata alimentadores y job  2) borra y recrea atmos-aire-demo  3) limpia metricas del Pushgateway  4) arranca el job con --limpiar
REM  5) publica de golpe las ultimas horas reales de la API (para que el modelo tenga 24 h de historia) y despues UN evento nuevo cada hora (hh:05).
REM No toca atmos-aire, atmos-aire-hist-* ni atmos-negocio.  Uso: scripts\reiniciar_demo_real.cmd
cd /d "%~dp0.."
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
echo [5/5] Alimentador REAL: ultimas horas de la API ahora y un evento nuevo cada hora (hh:05 UTC)...
docker exec -d lambda26-pyspark sh -c "echo $$ > /tmp/s12/alim.pid; python /tmp/s12/scripts/openmeteo_a_kafka.py --api --topic atmos-aire-demo --pausa 0.05; while true; do sleep $((3600 - $(date +%%s) %% 3600 + 300)); python /tmp/s12/scripts/openmeteo_a_kafka.py --api --topic atmos-aire-demo --desde $(date -u +%%Y-%%m-%%dT%%H:00); done > /tmp/s12/alimentador.log 2>&1"
echo.
echo LISTO. Grafana: http://localhost:43000 (tablero S12). Datos reales: las ultimas horas ya estan cargadas y llega un dato nuevo cada hora.
