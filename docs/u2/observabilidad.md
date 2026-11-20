# Observabilidad y alertas

## Qué se observa
| Capa | Fuente | Métricas / señal |
|---|---|---|
| Estado del job | Pushgateway (`push_time_seconds`) | Segundos desde el último lote; alerta `AireSinEventosNuevos` (> 10 min) |
| Datos | Job de Spark (`lambda26_aire_*`) | `pm25_ultimo`, `pred_pm25_proxima_hora`, `eventos_total`, `episodios_total`, `umbral_p95` |
| Modelo | Job de Spark | `mae_modelo`, `mae_persistencia`, `mejora_pct`, `n_evaluadas` |
| Streaming | `StreamingQueryListener` | `watermark_seconds`, `filas_descartadas_watermark_total`, `estado_filas`, filas por segundo |
| Broker | `kafka-exporter` | Mensajes por topic y retraso (*lag*) del consumidor |
| Plataforma | Healthchecks de Docker | Estado `(healthy)` por servicio; alerta `PushgatewayCaido` |

**Cadena:** Spark → Pushgateway → Prometheus → Grafana. El job de Spark es efímero, por eso se *empuja* en vez de scrapear.

## Alertas implementadas
Cinco reglas en `obs/prometheus/alerts.yml`, visibles en <http://localhost:49090/alerts>.

| Alerta | Condición | Severidad | Qué significa |
|---|---|---|---|
| `PushgatewayCaido` | `up{job="pushgateway"} == 0` durante 1 min | crítica | Sin Pushgateway no llegan métricas |
| `AireSinEventosNuevos` | Más de 600 s sin push del job, o la métrica no existe (`absent`) | advertencia | Fuente detenida o job caído |
| `AireModeloNoSuperaPersistencia` | `mejora_pct < 0` durante 10 min | advertencia | Deriva: reentrenar |
| `DescartesPorWatermarkCreciendo` | Aumentan los descartes en 30 min | informativa | Datos tardíos fuera de 2 h |
| `EpisodioDeAire` | `pm25_ultimo > umbral_p95` | informativa | Evento de negocio `aire.episodio` |

`AireSinEventosNuevos` usa `absent()` para disparar también si el job nunca empujó métricas.

## Semáforo de frescura (Grafana)
| Color | Segundos desde el último push |
|---|---|
| Verde | < 120 s |
| Naranja | ≥ 120 s |
| Rojo | ≥ 600 s |

## Umbrales operativos
| Dimensión | Métrica | Umbral |
|---|---|---|
| Latencia / frescura | `time() - push_time_seconds` | Rojo a 600 s |
| Throughput | `eventos_total` | Esperado: 1 evento por hora; si no crece por más de 2 h, la fuente o el productor están caídos |
| Errores | `filas_descartadas_watermark_total` | Cualquier valor > 0 se revisa (sube a propósito con `--tardio`) |

## Cómo provocar y ver una alerta
1. Detener el productor: la frescura supera 600 s, el semáforo pasa a rojo y `AireSinEventosNuevos` pasa a *Firing*.
2. Publicar de nuevo: la frescura baja, el semáforo vuelve a verde y la alerta se resuelve.
3. Publicar con `--tardio 30`: sube el contador de descartes y se dispara `DescartesPorWatermarkCreciendo`.

## Logs
Spark en nivel WARN y mensajes de cada script por consola; `docker logs lambda26-kafka --tail 100` para el broker. Los logs en JSON con campos fijos son una mejora pendiente: por ahora la información estructurada viaja en las métricas.
