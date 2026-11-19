# Producto U2 — Sistema en tiempo real (Aire)

## Pregunta de negocio
> ¿Un modelo entrenado con las últimas horas de PM2.5 y PM10 de Juliaca predice la concentración de PM2.5 de la próxima hora mejor que repetir la última hora (persistencia)?

**Por qué no «¿cuándo se superará el umbral?»:** en 2023 la norma peruana (ECA) **nunca** se excedió (0 h en PM2.5 y PM10) y la guía OMS (PM10 45 µg/m³) solo 4 h; esa pregunta casi no tiene casos positivos. Predecir la hora siguiente sí es medible en todas las horas. Alerta secundaria (relativa): *episodio* si PM2.5 > p95 de entrenamiento (10,5 µg/m³).

## Contrato del evento
Ver [Datos](datos.md#contrato-del-evento-kafka). Clave de partición = `sensorId`; `timestamp` = tiempo de evento.

## Consultas de Spark
| Consulta | Qué hace |
|---|---|
| `ventanas` | Ventana de **6 h**, watermark **2 h**, modo *append*: n, PM2.5 prom/máx, PM10 prom, horas sobre el umbral → Parquet |
| `inferencia` (`foreachBatch`) | Features de 24 h → GBT → predicción a +1 h; al llegar la hora real, error del modelo y de la persistencia; `aire.episodio` → `atmos-negocio` |

El checkpoint (`_ckpt/ventanas`, `_ckpt/inferencia`) permite reanudar sin duplicar; el lote de inferencia es **idempotente** (`bid <= ultimo` se ignora).

## Modelo y resultado
* Objetivo `delta = PM2.5(t+1) − PM2.5(t)`; 8 features calculadas por **una sola función** (`aire_comun.features`).
* Entrenamiento: GBT, pérdida absoluta, `maxIter=80`, `maxDepth=4`, `stepSize=0.1`; **solo se guarda si gana a la persistencia**.
* Prueba cronológica: **0,473 vs 0,687 (−31,1 %)**; lineal 0,742 (peor).
* **En vivo** (replay de dic-2023 + últimas horas de la API, fuera de muestra): **0,422 vs 0,567 (−25,5 %)**, 761 h evaluadas.

## Observabilidad
Tablero Grafana con estado del job (segundos desde el último lote), PM2.5 y predicción, MAE modelo vs persistencia, watermark y descartes. Ocho alertas en `obs/prometheus/alerts.yml`. Detalle y costos: [Observabilidad y costos](observabilidad-costos.md).
