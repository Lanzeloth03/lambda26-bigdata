# Producto U3 — Integración, DataOps y despliegue (Producto Final)

## Componentes mínimos y dónde se cumplen
| # | Requisito | Cumplimiento | Evidencia |
|---|---|---|---|
| 1 | Problema delimitado y arquitectura justificada | Lambda, pregunta a +1 h | [Arquitectura](arquitectura.md) |
| 2 | Pipeline batch con Spark y calidad | S2–S4, reglas de calidad | [Producto U1](u1-producto.md) |
| 3 | Salida en Parquet particionada | Gold por `mes` | [Datos](datos.md) |
| 4 | Ingesta con Kafka | `atmos-aire`, `atmos-negocio` | `openmeteo_a_kafka.py` |
| 5 | Streaming con ventanas y watermark | 6 h / 2 h | `s12_streaming_aire.py` |
| 6 | Observabilidad y costos | Prometheus, Grafana, 8 alertas | [Observabilidad y costos](observabilidad-costos.md) |
| 7 | Modelo ML / series de tiempo | GBT vs persistencia | [Pruebas](pruebas.md) |
| 8 | **DataOps/DevOps** | healthchecks, `service_healthy`, `.env`, compuertas, manifiesto | [Hardening](hardening.md), cuaderno 13 |
| 9 | **Revisión técnica final** | checklist, no regresión, riesgos aceptados | [No regresión y riesgos](noregresion-riesgos.md), [Bitácora](bitacora.md) |
| 10 | Matriz de trazabilidad | 12 filas con evidencia | [Matriz](trazabilidad.md) |
| 11 | Evidencias reproducibles | manifiestos JSON por corrida | [Corridas](evidencias/corridas.md) |
| 12 | Documentación y demo end-to-end | este sitio + guion con plan B | [Demo](demo.md) |

## Mapa a la rúbrica de la unidad
| Criterio | Peso | Dónde se demuestra |
|---|---|---|
| Integración end-to-end batch + streaming + BI/ML | 20 % | Cuaderno 13: batch validado → streaming con el modelo → MAE → Prometheus → tablero |
| Prácticas DataOps/DevOps | 15 % | Compose con salud y dependencias, `.env`, `levantar.cmd`, compuertas y manifiesto |
| Documentación operativa | 15 % | MkDocs de 10 secciones + [Instalación](ejecucion.md) con tabla «Si algo falla» |
| Hardening y revisión técnica | 10 % | [Checklist](hardening.md) antes/después, no regresión, riesgos con «qué cambia en producción» |
| Demo reproducible | 15 % | [Guion](demo.md) paso a paso con plan B; segunda corrida comparada |
| Sustentación técnica | 25 % | Presentación de 14 momentos, aporte individual y repositorio ([Informe](informe.md)) |

## Sustentación (23 min)
3 min pitch · 10 min exposición · 5 min demo · 5 min preguntas.
