# S12 EcoData U2: aire (Lanzeloth) + clima (Alahin)

Dos perfiles con la misma arquitectura: Open-Meteo → Kafka → Spark Structured Streaming → Pushgateway → Prometheus → Grafana.

| | Aire | Clima |
|---|---|---|
| Topic | `atmos-aire` | `atmos-clima` |
| Publicador | `openmeteo_a_kafka.py` | `openmeteo_clima_a_kafka.py` |
| Entrenamiento | `entrenar_pm25.py` | `entrenar_temp.py` |
| Job de Spark | `s12_streaming_aire.py` | `s12_streaming_clima.py` |
| Evento de negocio | `aire.episodio` (PM2.5 > p95) | `clima.helada` (T < 0 °C) |
| Prefijo de métricas | `lambda26_aire_` | `lambda26_clima_` |

Tablero único: `grafana/dashboard_s12_ecodata.json` (uid `lambda26-s12-ecodata`).

## Preparación (una vez)
1. Descargar el JSON de clima en el navegador y guardarlo como `clima_juliaca_2023.json` dentro de `s12-aire`:
   `https://archive-api.open-meteo.com/v1/archive?latitude=-15.5&longitude=-70.13&start_date=2023-01-01&end_date=2023-12-31&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&timezone=GMT`
2. `docker cp s12-aire lambda26-pyspark:/tmp/s12`
3. Crear el topic: `docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic atmos-clima --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092`
4. Entrenar el clima: `docker exec -it lambda26-pyspark python /tmp/s12/scripts/entrenar_temp.py --archivo /tmp/s12/clima_juliaca_2023.json --salida /tmp/s12/modelo_temp`

## Job de clima (otra terminal; deja corriendo el del aire)
`docker exec -it lambda26-pyspark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /tmp/s12/scripts/s12_streaming_clima.py --modelo /tmp/s12/modelo_temp --limpiar`

## Publicar el clima, en este orden
1. Replay: `docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_clima_a_kafka.py --archivo /tmp/s12/clima_juliaca_2023.json --desde 2023-12-01T00:00 --pausa 0.2`
2. API (últimas horas): `docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_clima_a_kafka.py --api`
3. Dato tardío para el watermark (con el job corriendo): `docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_clima_a_kafka.py --api --tardio 30`

Para probar `clima.helada` si no hay horas bajo 0 °C, lanza el job con `--umbral-helada 5` (ajusta al rango real).
