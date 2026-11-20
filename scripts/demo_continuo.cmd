@echo off
REM Mantiene los tableros de Grafana con datos llegando de forma continua (corre dentro del contenedor, no necesita una terminal abierta).
REM  1) crea el topic atmos-aire-demo  2) arranca el job de streaming  3) alimenta 1 evento cada 10 s (replay desde 2023-10-20, ~4,9 h) y luego la API cada hora.
REM Requisito: haber ejecutado antes scripts\levantar.cmd. Para parar: scripts\detener_demo.cmd
cd /d "%~dp0.."
echo [1/3] Topic atmos-aire-demo...
docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic atmos-aire-demo --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
echo [2/3] Job de streaming (sigue solo en el contenedor)...
docker exec -d lambda26-pyspark sh -c "[ -f /tmp/s12/job.pid ] && kill -0 $(cat /tmp/s12/job.pid) 2>/dev/null && exit 0; echo $$ > /tmp/s12/job.pid; exec spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /tmp/s12/scripts/s12_streaming_aire.py --modelo /tmp/s12/modelo_pm25 --topic atmos-aire-demo --limpiar > /tmp/s12/job_aire.log 2>&1"
timeout /t 40 /nobreak >nul
echo [3/3] Alimentador continuo (1 evento por minuto)...
docker exec -d lambda26-pyspark sh -c "[ -f /tmp/s12/alim.pid ] && kill -0 $(cat /tmp/s12/alim.pid) 2>/dev/null && exit 0; echo $$ > /tmp/s12/alim.pid; python /tmp/s12/scripts/openmeteo_a_kafka.py --archivo /tmp/s12/aq_juliaca_2023.json --desde 2023-10-20T00:00 --pausa 10 --topic atmos-aire-demo; while true; do python /tmp/s12/scripts/openmeteo_a_kafka.py --api --topic atmos-aire-demo --desde $(date -u -d '2 hours ago' +%Y-%m-%dT%H:00); sleep 3600; done > /tmp/s12/alimentador.log 2>&1"
echo.
echo LISTO. Grafana: http://localhost:43000  (tablero "S12 - Calidad del aire")
echo Los paneles se van poblando cada minuto. Deja la laptop encendida y conectada.
