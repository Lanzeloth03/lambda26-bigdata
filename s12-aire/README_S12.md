# S12 EcoData Aire (U2)

Open-Meteo → Kafka → Spark Structured Streaming → Pushgateway → Prometheus → Grafana.

| | Aire |
|---|---|
| Topic | `atmos-aire` |
| Publicador | `openmeteo_a_kafka.py` |
| Entrenamiento | `entrenar_pm25.py` |
| Job de Spark | `s12_streaming_aire.py` |
| Evento de negocio | `aire.episodio` (PM2.5 > p95) |
| Prefijo de métricas | `lambda26_aire_` |

Tablero: `grafana/dashboard_s12_aire.json` (uid `lambda26-s12-aire`).

## Preparación (una vez)
1. `docker cp s12-aire lambda26-pyspark:/tmp/s12`
2. Entrenar: `docker exec -it lambda26-pyspark python /tmp/s12/scripts/entrenar_pm25.py --archivo /tmp/s12/aq_juliaca_2023.json --salida /tmp/s12/modelo_pm25`

## Job de aire (deja esta terminal corriendo)
`docker exec -it lambda26-pyspark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /tmp/s12/scripts/s12_streaming_aire.py --modelo /tmp/s12/modelo_pm25 --limpiar`

## Publicar, en este orden
1. Replay: `docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_a_kafka.py --archivo /tmp/s12/aq_juliaca_2023.json --desde 2023-12-01T00:00 --pausa 0.2`
2. API (últimas horas): `docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_a_kafka.py --api`
3. Dato tardío para el watermark (con el job corriendo): `docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_a_kafka.py --api --tardio 30`
