# s12-integracion — Unidad 2 (Lanzeloth Vargas Marichi)

Pipeline en tiempo real: ESP32 + HC-SR04 → MQTT → Kafka → Spark Structured Streaming → Pushgateway → Prometheus → Grafana.

| Carpeta | Contenido |
|---|---|
| `scripts/s12_streaming_u2.py` | Job persistente: inferencia por evento, ventana + watermark, eventos de negocio, checkpoint |
| `scripts/s6_eventos_negocio.py` | Eventos de negocio en Kafka: `topico`, `servicio`, `orden`, `consumir` |
| `scripts/inyectar_tardio.py` | Publica un evento tardío para demostrar el watermark |
| `scripts/reentrenar_lr.py` | Reentrena el LR con el régimen reciente y lo valida contra la persistencia |
| `grafana/dashboard_s12.json` | Tablero importable «S12 - Pipeline U2 en vivo» (28 paneles) |
| `docs/u2/` + `mkdocs.yml` | Documentación (MkDocs Material) |

Orden de uso y comandos: `docs/u2/reproducibilidad.md`.

## Qué se probó y qué no
Probado con Spark 4.2.1 y fuente de archivos: inferencia con el `PipelineModel`, ventanas con watermark, reinicio con checkpoint, descarte de dato tardío (`numRowsDroppedByWatermark`), consulta de negocio, formato de métricas para Pushgateway y reentrenamiento.
Sin probar: lectura real de Kafka, `kafka-python` en el contenedor, importación del tablero en Grafana.
