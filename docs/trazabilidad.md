# Matriz de trazabilidad

**Variable:** concentración de PM2.5 en Juliaca · **Pregunta central:** ¿el modelo predice el PM2.5 de la próxima hora mejor que la persistencia? · **Integrante / dimensión:** Lanzeloth — Aire.

Competencias: **CE042** Construye Dataset · **CE043** Genera Modelos · **CE044** Analiza y Define Estrategias (nivel 3).

| # | Unidad | Indicador | Fuente y técnica | Evidencia (archivo / dónde se ve) | CE |
|---|---|---|---|---|---|
| 1 | U1 | Calidad del dato 2023: 8 760 h, 0 duplicados, 0 nulos, continuidad 100 % | Open-Meteo CAMS; PySpark, reglas Bronze→Silver | `u1-aire/S3_aire_calidad_parquet.ipynb`; `u3-integracion/13_…` (C1–C3) | CE042 |
| 2 | U1 | Gold Parquet particionado por mes, lectura con poda de partición | PySpark `partitionBy(mes)` | `u1-aire_corrida/gold/aire_mensual`; plan con `PartitionFilters mes=2023-06` | CE042 |
| 3 | U1 | Descriptivo: PM2.5 mensual y diario (p50/p95) | agregaciones Spark | `u1-aire_corrida/fig/mensual.png`, `diario.png` | CE044 |
| 4 | U1 | Predictivo: MAE a +1 h = **0,4635** vs persistencia 0,6871 (−32,5 %) | MLlib, 8 modelos, split cronológico | `fig/metricas.csv`, `modelo_aire_u1/meta.json`, `S4_aire_modelo_mllib.ipynb` | CE043 |
| 5 | U2 | Ingesta evento a evento con contrato `aire.lectura` | Kafka `atmos-aire`, productor Python | `s12-aire/scripts/openmeteo_a_kafka.py`; Kafka UI | CE042 |
| 6 | U2 | Ventanas 6 h + watermark 2 h; descartes por watermark | Structured Streaming | `s12_streaming_aire.py`; panel *watermark* en Grafana | CE042 |
| 7 | U2 | Predictivo en vivo: MAE **0,422** vs 0,567 (−25,5 %), 761 h | `foreachBatch` + MLlib | tablero `s12-aire/grafana/dashboard_s12_aire.json` | CE043 |
| 8 | U2 | Observabilidad: métricas, alertas, costos | Pushgateway→Prometheus→Grafana | `obs/prometheus/alerts.yml`; [Observabilidad y costos](observabilidad-costos.md) | CE044 |
| 9 | U3 | Integración end-to-end con compuertas (19) y manifiesto | cuaderno 13 | `u3-integracion/manifiestos/` · [Corridas](evidencias/corridas.md) | CE042/043/044 |
| 10 | U3 | DataOps: healthchecks, dependencias por salud, credenciales por variable | Compose | `kafka/compose.yml`, `obs/compose.yml`, `.env.example` | apoyo |
| 11 | U3 | Hardening, no regresión, riesgos aceptados, bitácora | revisión técnica | [Hardening](hardening.md), [No regresión](noregresion-riesgos.md), [Bitácora](bitacora.md) | apoyo |
| 12 | U3 | Demo reproducible con plan B | guion | [Demo](demo.md) | CE044 |
