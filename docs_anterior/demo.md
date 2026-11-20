# Demo end-to-end y plan B

**Duración:** 5 min dentro de los 23 de la sustentación. Se reproduce siguiendo [Instalación](ejecucion.md); cualquier persona puede repetirla.

## Antes de entrar (T−15 min)
- [ ] Docker Desktop *Engine running*, cargador conectado, sin otros kernels de Jupyter abiertos.
- [ ] `scripts\levantar.cmd` termina en `LISTO`; `docker ps` todo `(healthy)`.
- [ ] Grafana con los tableros *S12 Aire* y *U3 Integración* importados; refresco 5 s; *Last 15 minutes*.
- [ ] Cuaderno 13 ya ejecutado **una vez** (así la segunda corrida en vivo ya se compara con la primera).
- [ ] Capturas de respaldo abiertas en otra ventana (plan B).

## Guion (5 min)
| Min | Acción | Qué dice | Qué se ve |
|---|---|---|---|
| 0:00 | `docker ps` | «Cinco servicios con healthcheck; puertos solo en loopback» | `(healthy)` |
| 0:30 | Jupyter → cuaderno 13 → *Restart & Run All* | «Seis previas, calidad y plausibilidad; dos fallas a propósito que bloquean» | `[BLOQUEA]` en las demos, `[OK]` en lo real |
| 1:30 | Mientras corre el streaming, abrir Kafka UI | «Cada corrida tiene su topic» | topic `atmos-aire-int-<runId>` con 168 mensajes |
| 2:30 | MAE de producción | «El modelo gana a la persistencia en producción; el MAE se recalcula desde el Parquet» | 143 horas; MAE modelo < persistencia |
| 3:15 | Confirmación Prometheus | «Tres valores coinciden: lo que publicó el job, el Pushgateway y Prometheus» | O1–O3 `[OK]` |
| 3:45 | Tabla de 2 corridas | «Misma ventana, mismo modelo: métricas idénticas» | «¿equivalentes? SÍ» |
| 4:15 | Grafana *U3 Integración* | «El tablero refleja la corrida» | MAE, mejora %, compuertas |
| 4:45 | Cierre | «Riesgos aceptados y qué cambia en producción» | [Riesgos](noregresion-riesgos.md) |

## Plan B (si algo falla)
| Falla | Plan B | Tiempo |
|---|---|---|
| Docker no levanta o va lento | Mostrar las **capturas con hora visible** del anexo y el último `manifest_*.json` (archivo en `u3-integracion/manifiestos/`) | 0 |
| Kafka no responde | Ejecutar el cuaderno con `FUENTE=archivos` (lee el JSON local; la compuerta P1 queda como *omitida* y se dice explícitamente) | 1 min |
| Prometheus no confirma en 90 s | Abrir <http://localhost:49090/graph> y consultar `lambda26_aire_mae_modelo{job="lambda26_aire_int"}` a mano; explicar el intervalo de scrape de 15 s | 1 min |
| Grafana vacío | Recargar el tablero con *Last 3 hours*; si no, mostrar la salida del cuaderno | 30 s |
| Internet caído | La demo no depende de Internet: usa el JSON local (la API en vivo es opcional) | 0 |

**Regla de oro:** si algo falla, decirlo, mostrar la evidencia y seguir; no improvisar arreglos delante del jurado.
