# Dimensión U2: temperatura (Open-Meteo → Kafka → Spark)

Dimensión de **Alahin Reyme Ticona Veliz**, continuación de su dimensión de U1 (modelo RandomForest de temperatura con viento, humedad, hora y mes; RMSE 1.4351, R² 0.9180, MAE 1.1220 sobre 26 304 registros).

## Pregunta de negocio
> **¿Un modelo entrenado con las últimas horas de temperatura, humedad y viento de Juliaca predice la temperatura de la próxima hora mejor que repetir la última hora (persistencia) y mejor que repetir el cambio de ayer a la misma hora (persistencia diaria)?**

Alerta (regla absoluta): se emite `clima.helada` cuando la temperatura a 2 m baja de **0 °C**.

## De U1 a U2
En U1 la temperatura se estimaba con variables contemporáneas (viento, humedad, hora, mes) sobre el histórico completo. En U2 el objetivo cambia a **pronosticar la hora siguiente** con un flujo que llega evento a evento, y el error se mide con el dato que llega después. Como la temperatura tiene un ciclo diurno marcado, además de la persistencia se mide una **persistencia diaria** (baseline más exigente).

## Fuente (declarada con honestidad)
Open-Meteo (`temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`), horario, en GMT, lat −15.5, lon −70.13. Para entrenar y reproducir se usa la API de archivo (reanálisis); para las horas de hoy, la API de pronóstico (`past_days=2`). **Son modelos distintos**, así que puede haber un pequeño desajuste entre entrenamiento y vivo. No es una estación meteorológica.

## Arquitectura
`openmeteo_clima_a_kafka.py` → topic **`atmos-clima`** (key = sensorId) → Spark Structured Streaming (`s12_streaming_clima.py`):

| Consulta | Qué hace |
|---|---|
| `ventanas` | Ventana fija de **6 h** + **watermark de 2 h** (append): n, temperatura prom/máx/mín, humedad prom, horas bajo el umbral de helada. |
| `inferencia` (foreachBatch) | Features de las últimas 24 h → modelo MLlib → temperatura a +1 h; cuando llega la hora real se calcula el error y se compara con persistencia. Heladas → `atmos-negocio`. |
| métricas | Pushgateway `lambda26_clima_*` → Prometheus → Grafana (`dashboard_s12_ecodata.json`, sección Clima). |

Contrato del evento: `{"tipoEvento":"clima.lectura","sensorId":"openmeteo-clima-juliaca","temp":9.4,"hum":41,"viento":8.2,"origen":"openmeteo-replay|openmeteo-api|demo-tardio","timestamp":<ms de la hora del dato>}`.

## Modelo y resultados
- Objetivo: `delta = T(t+1) − T(t)`. Features: `l0, d1, d3, d24, m24, hum, humd, viento, hs, hc` (nivel, diferencias a 1/3/24 h, media 24 h, humedad y su cambio, viento, hora del día seno/coseno). Una sola función (`clima_comun.features`) las calcula en entrenamiento y en servicio.
- Split **cronológico** 80/20. `GBTRegressor(lossType="absolute")`.

| Modelo | MAE test (°C) |
|---|---|
| Persistencia | 1.096 |
| Persistencia diaria (baseline estacional) | 0.698 |
| Regresión lineal | 0.632 |
| **GBT, pérdida absoluta** | **0.537 (−51.0 % vs persistencia, −23.1 % vs persistencia diaria)** |

- n_train = 6 988 y n_test = 1 747 horas (split cronológico, test desde 2023-10-20).
- En streaming (replay de diciembre 2023 más las últimas horas de la API, posteriores al entrenamiento): MAE modelo 0.536 °C frente a 0.982 °C de la persistencia (−45.5 %) sobre 757 horas evaluadas.
- Hallazgo: a diferencia del aire, aquí la regresión lineal sí supera a la persistencia, pero el GBT sigue siendo el mejor de los tres.
- No hubo heladas (T < 0 °C) en el periodo reproducido, así que `clima.helada` no se disparó en la prueba en vivo.

## Limitaciones
1. Dato de modelo, no medido. 2. Un solo año de entrenamiento y una ubicación. 3. Archivo y pronóstico usan modelos distintos. 4. La historia de 24 h del pronóstico vive en memoria del job: tras un reinicio se reconstruye con las siguientes 24 horas de eventos.
