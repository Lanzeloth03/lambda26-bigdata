# lambda26-bigdata

Pipeline Big Data en tiempo real del curso (UPeU): **datos reales de un ESP32 con sensor DHT11** que viajan por MQTT hasta Kafka y se procesan con Spark Structured Streaming.

## Arquitectura

```
ESP32 + DHT11 --WiFi--> broker MQTT público --> puente (Python) --> Kafka --> Spark Structured Streaming --> Parquet
                        (test.mosquitto.org)                       topic: clima-real-eventos
```

| Pieza | Qué hace |
|---|---|
| `esp32_dht/` | Firmware Arduino: lee temperatura y humedad, sincroniza el reloj por NTP y publica un JSON cada 3 s. |
| `uso-atmos/` | Contenedor Python con el puente MQTT → Kafka (`bridge_mqtt_kafka.py`) y los productores/consumidores de S7. |
| `kafka/` | Kafka, Kafka UI y exportador de métricas (Docker Compose). |
| `pyspark/compose.kafka.yml` | Une el contenedor de PySpark a la red de Kafka. |
| `s08-streaming-estructurado/` | Notebook de la sesión 8 con las seis tareas sobre los datos reales. |

Contrato del evento (sin presión: el DHT11 no la mide y no se inventa):

```json
{"tipoEvento":"sensor.lectura","sensorId":"esp32-fisico-equipo01","temperatura":15.7,"humedad":36.0,"origen":"esp32-fisico","timestamp":1791384406000}
```

## Cómo reproducirlo

1. Levantar la infraestructura:
   ```
   docker compose -f kafka/compose.yml up -d
   docker compose -f uso-atmos/compose.yml up -d
   docker compose -f pyspark/compose.yml -f pyspark/compose.kafka.yml up -d
   ```
2. Crear el topic:
   ```
   docker compose -f kafka/compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic clima-real-eventos --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
   ```
3. Cargar `esp32_dht/esp32_dht.ino` en el ESP32 **poniendo antes tu WiFi** (`WIFI_SSID`, `WIFI_PASSWORD`) y un `EQUIPO` propio (el broker es público).
4. Arrancar el puente apuntando al topic de datos reales:
   ```
   docker compose -f uso-atmos/compose.yml exec -e MQTT_TOPIC=lambda26/atmos-real/equipo01/lecturas -e KAFKA_TOPIC_ATMOS=clima-real-eventos uso-atmos python /app/bridge_mqtt_kafka.py
   ```
5. Abrir Jupyter (token con `docker logs lambda26-pyspark`) y ejecutar `s08-streaming-estructurado/08_streaming_estructurado_autonoma.ipynb`.

## Qué demuestra el notebook de la S8

- Lectura en streaming con **esquema explícito** y evidencia de que los datos vienen del sensor físico.
- Agregación por ventana fija de 20 s con **watermark de 10 s**, con el margen justificado midiendo la latencia real (p95).
- Un **dato tardío** descartado por el watermark (con un evento de control que sí aparece).
- Un **duplicado** publicado dos veces y contado una sola vez con `dropDuplicates` acotado por watermark.
- Comparación de **dos intervalos de disparo** (1 s y 6 s) con el `batchId` alcanzado en 15 s.
- Salida en **Parquet con checkpoint**, verificada en dos corridas que acumulan; inspección de `offsets/`, `commits/` y `state/`.

## Hallazgos

- Se detectó un **hueco de ~8 minutos** en las lecturas reales con una función de ventana sobre `offset`; los datos no llegaron tarde, se perdieron antes de Kafka.
- Una agregación **sin deduplicar** cuenta un evento duplicado dos veces (`n = 2`).
- Tras detener una consulta a mitad de un micro-lote, el checkpoint tenía `offsets/` un lote por delante de `commits/`; Spark lo reejecutó al reiniciar.

## Seguridad

Las credenciales de WiFi **no** están en el repositorio: el sketch trae `TU_WIFI` y `TU_CLAVE` como marcadores.
