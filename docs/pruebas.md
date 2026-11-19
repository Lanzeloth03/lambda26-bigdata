# 7. Pruebas y evidencias

Cada afirmación del proyecto tiene una prueba. La columna **Dónde se ve** dice qué abrir para comprobarlo.

## Resultados de modelo (medidos)
| Prueba | Resultado | Dónde se ve |
|---|---|---|
| Batch U1: GBT depth 3 vs persistencia (test cronológico 1 747 h) | MAE **0,4635** vs 0,6871 (**−32,5 %**), R² 0,9261 | `u1-aire_corrida/fig/metricas.csv`, `modelo_aire_u1/meta.json`, notebook S4 |
| Comparativa de modelos U1 | lineal 0,7419 y Ridge 0,7459 **peores** que persistencia; RF 0,4780; GBT d4 0,4734; d5 0,4813 | `metricas.csv` |
| Speed U2: modelo del job (GBT d4, pérdida absoluta) | MAE 0,473 vs 0,687 (−31,1 %) en prueba cronológica | `s12-aire/modelo_pm25/meta.json` |
| Speed U2 en vivo (replay dic-2023 + API, fuera de muestra) | MAE **0,422** vs 0,567 (**−25,5 %**), 761 h evaluadas | [Dimensión U2](u2-producto.md) |
| Validación en 4 tramos cronológicos | el GBT mejora 25–32 % en todos | notas U2 |

## Pruebas de integración (U3)
El cuaderno `13_integracion_dataops.ipynb` ejecuta **19 compuertas** (6 previas, 3 calidad, 2 plausibilidad, 5 streaming, 3 observabilidad) y guarda un manifiesto por corrida. Las corridas reales del laboratorio se listan en [Corridas de integración](evidencias/corridas.md) (generada con `scripts/resumen_manifiestos.py`).

| Compuerta | Tipo | Bloquea | Criterio |
|---|---|---|---|
| P1–P6 | previa | sí | Kafka, Pushgateway, Prometheus, target `pushgateway` UP, modelo y datos presentes |
| C1–C3 | calidad | sí | 0 duplicados · 0 negativos · continuidad ≥ 99 % |
| PL1 | plausibilidad | **sí** | mediana PM2.5 ∈ [1, 15] µg/m³ |
| PL2 | plausibilidad | **no (advierte)** | p95 PM2.5 ≤ 30 µg/m³ (un episodio alto puede ser real) |
| S1–S2 | streaming | sí | publicación y job terminan, con resumen |
| S3 | streaming | sí | 168 h − 24 h de historia − 1 = **143** horas evaluadas |
| S4 | streaming | sí | MAE reportado por el job = MAE recalculado desde el Parquet (±1e-9) |
| S5 | streaming | sí | MAE modelo < MAE persistencia **en producción** |
| O1–O3 | observabilidad | sí | valor publicado = valor en Prometheus = valor en Parquet |

**Fallas demostradas a propósito (el cuaderno las ejecuta y muestra que bloquean):**
1. Previa P2 apuntando a un Pushgateway inexistente (puerto 9999) → *BLOQUEA*.
2. Copia corrupta de la serie (hora duplicada + PM2.5 negativo) → C1 *BLOQUEA*; la serie real queda intacta.

KPI con **p50 / p95** del error absoluto (no medias): el manifiesto guarda `err_abs_modelo_p50/p95` y `err_abs_persistencia_p50/p95`.

## Corrida real en el laboratorio (2026-11-19, Docker Desktop, Kafka + Prometheus reales)
Dos ejecuciones consecutivas del cuaderno 13 con `FUENTE=kafka`: **19/19 compuertas OK en ambas**, 168 eventos publicados a un topic propio, **143 horas evaluadas**, MAE del modelo **0,4551** vs persistencia **0,6524** (−30,2 %) en la ventana fija de dic-2023, p50/p95 del error absoluto 0,2665 / 1,5020, y las dos corridas **idénticas** en métrica. El valor de `mae_modelo` coincidió entre el job, el Pushgateway, Prometheus y el Parquet. Detalle en [Corridas de integración](evidencias/corridas.md).

## Verificación durante el desarrollo (sin Docker)
Antes de entregar, el cuaderno se ejecutó **dos veces** con `FUENTE=archivos` y un Pushgateway/Prometheus simulados (mocks) en un entorno sin Docker: **19/19 compuertas OK** en ambas, métricas **idénticas** entre corridas (143 horas, mismo MAE y p95). Esa prueba valida la lógica del cuaderno, **no sustituye** la corrida real con Kafka, Pushgateway y Prometheus del laboratorio (ver corridas).

## Pruebas de infraestructura
| Prueba | Comando | Esperado |
|---|---|---|
| Servicios sanos | `scripts\probar_salud.cmd` | 5 `OK` + Kafka `healthy` |
| Estado de salud de contenedores | `docker ps --format "table {{.Names}}\t{{.Status}}"` | `(healthy)` en kafka, kafka-ui, prometheus, pushgateway, grafana |
| Orden de arranque | `docker compose -f obs/compose.yml up -d --wait` | Grafana espera a Prometheus *healthy* |
| Alertas cargadas | <http://localhost:49090/alerts> | 8 reglas del grupo `ecodata_pipeline` |
| Target UP | <http://localhost:49090/targets> | `pushgateway` UP |

## Capturas para el informe (con hora visible)
1. `levantar.cmd` terminando en `LISTO`. 2. `docker ps` con `(healthy)`. 3. Jupyter con las compuertas `[OK]` y el reloj del sistema. 4. Tabla de comparación de 2 corridas. 5. Grafana *U3 - Integración*. 6. Prometheus `/targets`. 7. Kafka UI con el topic de la corrida.
