# Corridas de integración

Página **generada** por `python scripts/resumen_manifiestos.py` desde `u3-integracion/manifiestos/`. No se edita a mano.

| runId (UTC) | fuente | horas eval. | MAE modelo | MAE persist. | mejora % | err p50 | err p95 | compuertas OK | duración (s) |
|---|---|---|---|---|---|---|---|---|---|
| `20261119T233309Z` | kafka | 143 | 0.455092 | 0.652448 | 30.25 | 0.2665 | 1.5020 | 19/19 | 46.4 |
| `20261119T233555Z` | kafka | 143 | 0.455092 | 0.652448 | 30.25 | 0.2665 | 1.5020 | 19/19 | 41.3 |

**Comparación de las dos últimas corridas:** idénticas en métrica (sin regresión). Δ duración = -5.1 s.

## Compuertas de la última corrida

| compuerta | fase | estado | detalle |
|---|---|---|---|
| P1 Kafka accesible | previa | OK | broker kafka:9092 acepta conexiones |
| P2 Pushgateway sano | previa | OK | HTTP 200 |
| P3 Prometheus sano | previa | OK | HTTP 200 |
| P4 Prometheus scrapea Pushgateway (UP) | previa | OK | targets pushgateway: ['up'] |
| P5 Modelo MLlib presente | previa | OK | /tmp/s12/modelo_pm25/meta.json |
| P6 Datos fuente presentes | previa | OK | /tmp/s12/aq_juliaca_2023.json (239843 bytes) |
| C1 sin duplicados (batch) | calidad | OK | 0 horas duplicadas |
| C2 sin negativos (batch) | calidad | OK | 0 valores < 0 |
| C3 continuidad horaria (batch) | calidad | OK | 100.00 % de pasos de 1 h (0 saltos) |
| PL1 mediana PM2.5 plausible (batch) | plausibilidad | OK | p50 = 4.80 µg/m³ (rango 1–15) |
| PL2 p95 PM2.5 plausible (batch) | plausibilidad | OK | p95 = 10.80 µg/m³ (aviso si > 30) |
| S1 publicación a Kafka | streaming | OK | 168 eventos -> atmos-aire-int-20261119t233555z (openmeteo-replay) |
| S2 streaming terminó y reportó resumen | streaming | OK | {'mae_modelo': 0.4550922056033264, 'mae_persistencia': 0.6524475524475521, 'n_evaluadas': 143, 'eventos': 168, |
| S3 horas evaluadas = 168 − 24 (historia) − 1 (sin siguiente) | streaming | OK | 143 horas evaluadas, esperado 143 |
| S4 MAE del job == MAE recalculado desde Parquet | streaming | OK | job=0.455092  parquet=0.455092 |
| S5 el modelo supera a la persistencia en producción | streaming | OK | MAE modelo 0.4551 < persistencia 0.6524 (30.2 % mejor) |
| O1 el job publicó mae_modelo en el Pushgateway | observabilidad | OK | Pushgateway=0.4550922056033264  Parquet=0.455092206 |
| O2 Prometheus devuelve el mismo valor que se publicó | observabilidad | OK | Prometheus=0.4550922056033264  Pushgateway=0.4550922056033264 |
| O3 n_evaluadas coincide (Pushgateway = Prometheus = Parquet) | observabilidad | OK | 143.0 / 143.0 / 143 |

## Fallas demostradas a propósito

* **P2 (demo) Pushgateway en puerto 9999** → BLOQUEADA como se esperaba: P2 (demo) Pushgateway en puerto 9999: puerto 9999 inexistente: URLError
* **C1/C2 calidad (copia corrupta)** → BLOQUEADA como se esperaba: C1 sin duplicados (copia corrupta): 1 horas duplicadas

## Observabilidad (última corrida)

```json
{
  "pushgateway_url": "http://pushgateway:9091",
  "prometheus_url": "http://prometheus:9090",
  "job": "lambda26_aire_int",
  "mae_modelo_publicado": 0.4550922056033264,
  "mae_modelo_prometheus": 0.4550922056033264,
  "n_evaluadas_prometheus": 143.0,
  "latencia_confirmacion_s": 0.0
}
```
