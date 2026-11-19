# Observabilidad y costos

## Qué se observa
| Capa | Fuente | Métricas / señal |
|---|---|---|
| Estado del job | Pushgateway (`push_time_seconds`) | segundos desde el último lote; alerta `AireSinEventosNuevos` (> 10 min) |
| Datos | job de Spark (`lambda26_aire_*`) | `pm25_ultimo`, `pred_pm25_proxima_hora`, `eventos_total`, `episodios_total` |
| Modelo | job de Spark | `mae_modelo`, `mae_persistencia`, `mejora_pct`, `n_evaluadas`; alerta si `mejora_pct < 0` por 10 min |
| Streaming | `StreamingQueryListener` | `watermark_seconds`, `filas_descartadas_watermark_total`, `estado_filas`, `input_rows_per_second` |
| Broker | `kafka-exporter` | mensajes por topic, lag |
| Plataforma | healthchecks de Docker | `(healthy)` por servicio; alerta `PushgatewayCaido` |
| Integración | cuaderno 13 | `lambda26_aire_int_*`, manifiesto JSON |

**Logs:** `docker logs lambda26-kafka --tail 100`, salida del job en su pestaña de CMD, salida de cada celda del cuaderno 13.

## Tableros
1. **S12 – Calidad del aire** (`s12-aire/grafana/dashboard_s12_aire.json`): operación en vivo.
2. **U3 – Integración Aire** (`u3-integracion/grafana/dashboard_u3_integracion.json`): 5 paneles de la última corrida (MAE modelo vs persistencia, mejora %, horas evaluadas, compuertas OK/total, duración).

## Alertas (Prometheus → <http://localhost:49090/alerts>)
`PushgatewayCaido`, `AireSinEventosNuevos`, `AireModeloNoSuperaPersistencia`, `DescartesPorWatermarkCreciendo`, `EpisodioDeAire` (+ equivalentes de clima del equipo).

## Estimación de costos
!!! info "Precios de referencia"
    AWS us-east-1: t3.medium US$ 0,0416/h; S3 Standard US$ 0,023/GB-mes. Son valores de referencia que deben verificarse en la página de precios antes de usarlos como presupuesto.

**Volumen:** 1 evento por hora por ubicación (CAMS es horario) = 24 eventos/día/ubicación. El año 2023 completo son 8 760 eventos y el JSON ocupa 240 KB.

| Escenario | Carga | Cómputo | Almacenamiento | Total aprox. |
|---|---|---|---|---|
| Laboratorio (hoy) | 1 ubicación | laptop del equipo | < 1 MB | US$ 0 |
| Nube mínima | 1 ubicación | 1 × t3.medium × 730 h = **US$ 30,37/mes** (Kafka + Spark + Prometheus + Grafana en un host) | despreciable | **≈ US$ 30/mes** |
| 100 ubicaciones | 100 ev/h = 0,03 ev/s | mismo host (hipótesis a probar) | ≈ 24 MB/año (tamaño del JSON de origen) | ≈ US$ 30/mes |
| 1 000 ubicaciones | 0,28 ev/s | 3 brokers + Spark en clúster | ≈ 0,24 GB/año (JSON de origen) | no estimado: requiere prueba de carga |

El costo lo domina el **cómputo siempre encendido**, no los datos; con volumen tan bajo el trigger de 5 s y el arranque de Spark pesan más que la carga. Son **extrapolaciones lineales, no pruebas de carga**.

## Criterios de escalado
| Señal | Umbral | Acción |
|---|---|---|
| `lote_id` deja de avanzar | > 10 min | revisar job y fuente |
| Filas de estado crecen sin techo | sostenido | revisar watermark (debe acotar el estado) |
| Lag del consumidor | > 10 durante 2 min | más particiones/ejecutores |
| `mejora_pct` < 0 | 10 min | reentrenar (deriva) |
