# Observabilidad y alertas (S9)

## Cadena
Spark (2 jobs) → **Pushgateway** → **Prometheus** (scrape cada 15 s, `honor_labels: true`) → **Grafana** (`lambda26-s12-ecodata`) y **alertas** (`alerts.yml`).

## Métricas por dimensión
| Tipo | Métrica | Dónde |
|---|---|---|
| Disponibilidad | `up{job="pushgateway"}` | Prometheus → Targets |
| Latencia / frescura | `time() - push_time_seconds{instance="inferencia"}` | Semáforo (verde < 120 s, naranja ≥ 120 s, rojo ≥ 600 s) |
| Throughput | `input_rows_per_second` | Panel *Estado y descartes del watermark* |
| Calidad del modelo | `mae_modelo`, `mae_persistencia`, `mejora_pct` | Panel *Error: modelo vs persistencia* |
| Streaming | `filas_descartadas_watermark_total`, `estado_filas` | Panel de watermark |
| Negocio | `episodios_total`, `heladas_total` | Paneles de episodios |

## Alertas (`obs/prometheus/alerts.yml`)
| Alerta | Condición | Severidad |
|---|---|---|
| `PushgatewayCaido` | Pushgateway inaccesible 1 min | crítica |
| `AireSinEventosNuevos` / `ClimaSinEventosNuevos` | > 10 min sin lote | advertencia |
| `AireModeloNoSuperaPersistencia` / `Clima…` | `mejora_pct < 0` por 10 min | advertencia |
| `DescartesPorWatermarkCreciendo` | aparecen descartes | informativa |
| `EpisodioDeAire` / `HeladaPrevista` | evento de negocio activo | informativa |

## Correlación para el jurado
1. Detén la fuente (o el job): la **frescura** sube, el **semáforo** pasa a rojo y a los 10 min se dispara `AireSinEventosNuevos`.
2. Vuelve a publicar: el job reanuda y la alerta se resuelve.
3. Publica con `--tardio 30`: suben `filas_descartadas_watermark_total` y se dispara la alerta informativa.

## Cómo activarlas
1. Copiar `alerts.yml` a `D:\ACUÑA\lambda26-bigdata\obs\prometheus\`.
2. Agregar `rule_files: [/etc/prometheus/alerts.yml]` al `prometheus.yml` (ver `prometheus_fragmento.yml`).
3. `docker restart lambda26-prometheus` y abrir http://localhost:49090/alerts.

## Por qué Pushgateway
Los jobs empujan métricas por lote. Pushgateway está pensado para jobs que terminan, y un streaming no termina: si el job muere, la última métrica queda «congelada» y Prometheus no lo nota. **Lo mitigamos con el semáforo de frescura** (`push_time_seconds`) y la alerta `...SinEventosNuevos`. La alternativa más limpia sería que el job exponga `/metrics` y Prometheus lo lea, como mejora futura.
