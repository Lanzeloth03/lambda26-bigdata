# Cómo reproducir el pipeline

Requisitos: Docker Desktop, el repositorio `lambda26` con el stack (`kafka`, `obs`, `pyspark`), el ESP32 publicando y el puente MQTT → Kafka activo.

## 1. Levantar los servicios

```
docker start lambda26-kafka lambda26-kafka-ui lambda26-kafka-exporter lambda26-pyspark
docker start lambda26-prometheus lambda26-grafana lambda26-pushgateway
```

Grafana: <http://localhost:43000> · Prometheus: <http://localhost:49090> · Pushgateway: <http://localhost:49091>

## 2. Copiar los scripts y el modelo al contenedor

```
docker exec lambda26-pyspark mkdir -p /tmp/s12
docker cp D:\ACUÑA\lambda26-bigdata\s12-integracion\scripts\. lambda26-pyspark:/tmp/s12
docker cp <ruta-en-Windows>\modelo_distancia_lr lambda26-pyspark:/tmp/s12/modelo_distancia_lr
```

## 3. Eventos de negocio

```
docker exec lambda26-pyspark python /tmp/s12/s6_eventos_negocio.py topico
docker exec -it lambda26-pyspark python /tmp/s12/s6_eventos_negocio.py servicio
docker exec -it lambda26-pyspark python /tmp/s12/s6_eventos_negocio.py consumir
```

## 4. Job de streaming

```
docker exec -it lambda26-pyspark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /tmp/s12/s12_streaming_u2.py --modelo /tmp/s12/modelo_distancia_lr --limpiar
```

Sin `--limpiar` el job retoma desde su checkpoint.

## 5. Tablero

Grafana → Dashboards → New → Import → subir `grafana/dashboard_s12.json` → elegir el datasource Prometheus.

## Estructura del repositorio

```
s12-integracion/
├── scripts/      s12_streaming_u2.py · s6_eventos_negocio.py · inyectar_tardio.py · reentrenar_lr.py
├── grafana/      dashboard_s12.json
├── docs/u2/      esta documentación
└── mkdocs.yml
```

Publicar la documentación: `pip install mkdocs-material` y `mkdocs serve` (local) o `mkdocs gh-deploy` (GitHub Pages).
