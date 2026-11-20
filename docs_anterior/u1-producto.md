# Producto U1 — Batch distribuido (Aire)

| Requisito U1 | Cómo se cumple | Evidencia |
|---|---|---|
| Problema delimitado + arquitectura justificada | Lambda; pregunta de pronóstico a +1 h | [Arquitectura](arquitectura.md), `EcoData_S1_Arquitectura_Aire.pptx` |
| Pipeline con Spark: extracción, transformaciones, agregaciones, calidad | S2 (transformaciones, *lazy evaluation*), S3 (calidad y Parquet) | `u1-aire/S2_…`, `S3_…` |
| Salida analítica en Parquet particionada | Gold por `mes` (12 particiones) | `u1-aire_corrida/gold` |
| Modelo de regresión distribuido | S4: persistencia, lineal, Ridge, Elastic Net, RF, GBT d3/d4/d5 | `fig/metricas.csv` |

## Resultados (test cronológico: 1 747 h desde 2023-10-20)
| Modelo | RMSE | MAE | R² |
|---|---|---|---|
| Persistencia (baseline) | 1,5067 | 0,6871 | 0,8478 |
| Regresión lineal | 1,4185 | 0,7419 | 0,8651 |
| Ridge (L2) | 1,4374 | 0,7459 | 0,8615 |
| Elastic Net | 1,4353 | 0,7094 | 0,8619 |
| Random Forest | 1,0727 | 0,4780 | 0,9229 |
| **GBT depth=3 (ganador)** | **1,0498** | **0,4635** | **0,9261** |
| GBT depth=4 | 1,0656 | 0,4734 | 0,9239 |
| GBT depth=5 | 1,1322 | 0,4813 | 0,9141 |

**Lectura:** el modelo lineal minimiza el error cuadrático y por eso es *peor* que la persistencia en MAE; el GBT captura la no linealidad y mejora 32,5 %.

Notebook integrado: `u1-aire/u1_aire_lanzeloth.ipynb` · Ejecutado con evidencia: `u1-aire_corrida/`.
