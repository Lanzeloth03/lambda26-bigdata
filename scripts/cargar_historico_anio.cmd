@echo off
REM Descarga de Open-Meteo (CAMS, mismo punto de Juliaca) un ANIO real distinto de 2023 y lo publica en su propio topic atmos-aire-hist-<anio>.
REM Uso: scripts\cargar_historico_anio.cmd 2024 [FECHA_FIN]  (para 2026: scripts\cargar_historico_anio.cmd 2026 2026-11-20)      (no toca atmos-aire ni el demo; necesita internet dentro del contenedor)
cd /d "%~dp0.."
set A=%1
if "%A%"=="" set A=2024
set FIN=%2
if "%FIN%"=="" set FIN=%A%-12-31
docker exec lambda26-pyspark python -c "import urllib.request as u;open('/tmp/s12/aq_juliaca_%A%.json','wb').write(u.urlopen('https://air-quality-api.open-meteo.com/v1/air-quality?latitude=-15.5&longitude=-70.13&hourly=pm10,pm2_5&start_date=%A%-01-01&end_date=%FIN%&timezone=GMT',timeout=120).read());print('descargado %A%')"
docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic atmos-aire-hist-%A% --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_a_kafka.py --archivo /tmp/s12/aq_juliaca_%A%.json --pausa 0.01 --topic atmos-aire-hist-%A%
echo LISTO atmos-aire-hist-%A%. Verifica en Kafka UI.
