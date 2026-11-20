# Dimensión U2 reformulada: calidad del aire (Open-Meteo → Kafka → Spark)

## Pregunta de negocio
> **¿Un modelo entrenado con las últimas horas de PM2.5 y PM10 de Juliaca predice la concentración de PM2.5 de la próxima hora mejor que simplemente repetir la última hora (persistencia)?**

Alerta secundaria (relativa): se marca *episodio* cuando PM2.5 supera el **p95 del periodo de entrenamiento** (10.5 µg/m³ en 2023).

## Por qué esta pregunta y no el umbral ECA
Con los datos de 2023 (8 760 h, Juliaca) la norma peruana (ECA) **nunca** se excedió (0 h en PM2.5 y PM10) y la guía OMS (PM10 45 µg/m³) solo 4 h: una pregunta de "¿superó el umbral?" casi no tiene eventos que detectar. Predecir la hora siguiente sí tiene respuesta medible en todas las horas.

## Fuente de datos
Open-Meteo Air Quality API, datos **modelados CAMS** en grilla de ~45 km (lat −15.5, lon −70.13), cada hora, en GMT.

## Arquitectura
`openmeteo_a_kafka.py` → topic **`atmos-aire`** (key = sensorId) → Spark Structured Streaming:

| Consulta | Qué hace |
|---|---|
| `ventanas` | Ventana fija de **6 h** + **watermark de 2 h** (append): n, PM2.5 prom/máx, PM10 prom, horas sobre el umbral. |
| `inferencia` (foreachBatch) | Features de las últimas 24 h → modelo MLlib → predicción de PM2.5 a +1 h; cuando llega la hora real se calcula el error y se compara con persistencia. Episodios → `atmos-negocio`. |
| métricas | Pushgateway `lambda26_aire_*` → Prometheus → Grafana (`dashboard_s12_aire.json`, 16 paneles). |

Contrato del evento: `{"tipoEvento":"aire.lectura","sensorId":"openmeteo-cams-juliaca","pm25":6.5,"pm10":9.4,"origen":"openmeteo-replay|openmeteo-cams|demo-tardio","timestamp":<ms de la hora del dato>}`.

## Modelo y resultados (medidos)
- Objetivo: `delta = PM2.5(t+1) − PM2.5(t)`. Features: `l0, d1, d3, d24, m24, pm10d, hs, hc` (nivel, diferencias a 1/3/24 h, media 24 h, PM10−PM2.5, hora del día seno/coseno). **Una sola función (`aire_comun.features`) las calcula en entrenamiento y en servicio** → sin training-serving skew.
- Split **cronológico** 80/20 (test desde 2023-10-20). `GBTRegressor(lossType="absolute")`.

| Modelo | MAE test (µg/m³) |
|---|---|
| Persistencia (baseline) | 0.687 |
| Regresión lineal | 0.742 (**peor** que la baseline) |
| **GBT, pérdida absoluta** | **0.473 (−31.1 %)** |

- Validación en 4 tramos cronológicos (scikit-learn, mismo planteamiento): GBT mejora 25–32 % en todos.
- En streaming (replay de diciembre 2023 más las últimas horas de la API, posteriores al entrenamiento): MAE modelo 0.422 vs persistencia 0.567 (−25.5 %) sobre 761 horas evaluadas.
- Hallazgo: la regresión lineal optimiza error cuadrático y no supera a la persistencia en MAE; por eso se eligió un GBT con pérdida absoluta.

## Datos adicionales en Kafka
Además de 2023, se cargaron a Kafka los años **2024** (`atmos-aire-hist-2024`) y **2025** (`atmos-aire-hist-2025`) desde Open-Meteo, hora por hora y con el mismo contrato de evento. Ver [Datos y topics](datos-y-topics.md).

## Observabilidad y alertas
Cinco reglas en Prometheus (`PushgatewayCaido`, `AireSinEventosNuevos`, `AireModeloNoSuperaPersistencia`, `DescartesPorWatermarkCreciendo`, `EpisodioDeAire`) y semáforo de frescura en Grafana. Ver [Observabilidad y alertas](observabilidad-alertas.md).

## Integración reproducible
Una corrida de integración con 19 compuertas deja un manifiesto: 143 horas evaluadas, MAE 0,4551 frente a 0,6524 (−30,2 %), 19/19 compuertas OK. Ver [Integración y compuertas](integracion.md).

## Limitaciones
1. Dato modelado, no medido; grilla de ~45 km. 2. Un solo año (2023) y una sola ubicación. 3. El modelo se entrenó hasta oct-2023; el replay de dic-2023 y las horas de la API son posteriores, así que la métrica en vivo es fuera de muestra (MAE 0,422 frente a 0,567 de la persistencia, −25,5 % sobre 761 h; en prueba cronológica 0,473 frente a 0,687, −31,1 %), pero cubre un solo periodo. 4. Estacionalidad: PM10 de ene ≈ 2.4 vs ago ≈ 10.0 µg/m³ → reentrenar periódicamente (deriva).
