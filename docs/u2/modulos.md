# Módulos y métricas

| Módulo | Archivo | Qué hace |
|---|---|---|
| Contrato y features | `s12-aire/scripts/aire_comun.py` | Define el esquema del evento y construye las 8 features (`l0, d1, d3, d24, m24, pm10d, hs, hc`); una sola fuente de verdad en entrenamiento y servicio |
| Entrenamiento | `entrenar_pm25.py` | Partición cronológica 80/20; compara persistencia, lineal y GBT; **solo guarda el modelo si gana a la persistencia** |
| Productor | `openmeteo_a_kafka.py` | Publica el JSON histórico (replay) o la API en vivo a `atmos-aire`; `--tardio` prueba el watermark; `--desde/--hasta/--pausa` controlan el replay |
| Streaming | `s12_streaming_aire.py` | Consulta `ventanas` (6 h, watermark 2 h) y consulta `inferencia` (+1 h, MAE modelo vs persistencia, episodios) |
| Integración | `u3-integracion/13_integracion_dataops.ipynb` | Previas → calidad → streaming → MAE → Prometheus → manifiesto → comparación |
| Alertas | `obs/prometheus/alerts.yml` | `PushgatewayCaido`, `AireSinEventosNuevos`, `AireModeloNoSuperaPersistencia`, `DescartesPorWatermarkCreciendo`, `EpisodioDeAire` |
| Arranque y demo | `scripts/*.cmd` | Orden de arranque, espera por salud, topics, carga de histórico, demo continuo |

## Métricas publicadas (prefijo `lambda26_aire_`)
| Grupo | Métricas |
|---|---|
| Datos | `pm25_ultimo`, `pred_pm25_proxima_hora`, `eventos_total`, `episodios_total`, `umbral_p95` |
| Modelo | `mae_modelo`, `mae_persistencia`, `mejora_pct`, `n_evaluadas` |
| Streaming | `lote_id`, `ultima_hora_evento_seconds`, `filas_descartadas_watermark_total`, `watermark_seconds`, `ventana_*` |
| Integración (job `lambda26_aire_int`) | `_gates_ok`, `_gates_total`, `_duracion_total_seconds` |
| Broker (kafka-exporter) | `kafka_topic_partition_current_offset` (mensajes por topic) |
