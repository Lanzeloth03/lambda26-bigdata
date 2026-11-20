@echo off
REM Carga el ano completo 2023 (8 760 lecturas horarias) en el topic atmos-aire (capa Bronze en Kafka).
REM No toca atmos-aire-demo ni el job de streaming del demo. Tarda pocos minutos. Si el topic ya tiene datos, no se vuelve a cargar.
cd /d "%~dp0.."
docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic atmos-aire --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
for /f "tokens=3 delims=:" %%a in ('docker exec lambda26-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic atmos-aire --time -1') do set N=%%a
if not "%N%"=="0" (echo atmos-aire ya tiene %N% mensajes, no se recarga. & goto fin)
docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_a_kafka.py --archivo /tmp/s12/aq_juliaca_2023.json --pausa 0.01 --topic atmos-aire
:fin
echo.
echo LISTO. Verifica en Kafka UI: Topics - atmos-aire.
