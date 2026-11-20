# Preparación S12: evaluación de la Unidad II

Cada tema de S6 a S11 respondido con el pipeline de EcoData Aire.

## 1. Teoría S6 a S11 con ejemplos de EcoData

### S6 · Eventos empresariales con Kafka
| Concepto | En EcoData Aire |
|---|---|
| Broker | Un broker Kafka (contenedor `lambda26-kafka`), puerto 49092 |
| Topic | `atmos-aire` (telemetría) y `atmos-negocio` (eventos de negocio) |
| Partición y clave | 1 partición por topic; clave = `sensorId`, así los eventos de una fuente conservan su orden |
| Offset | Posición del mensaje; Spark guarda el último offset leído en su checkpoint |
| Contrato | JSON con esquema único en `aire_comun.py` ([Contrato de eventos](contrato-eventos.md)) |
| Evento empresarial | `aire.episodio`: nace de una regla de negocio, no de la fuente |

### S7 · Eventos IoT/sensores
- **Frecuencia y volumen:** 1 lectura por hora (dato modelado de Open-Meteo), tamaño constante ≈ 150 bytes.
- **Validación:** esquema y rango físico (PM ≥ 0).
- **Honestidad de la fuente:** el dato es modelado (CAMS, ≈ 45 km), no una estación; funciona como «sensor virtual».
- **¿Por qué un sensor real usa MQTT y no Kafka?** Un sensor tiene poca memoria, batería y conexión intermitente; MQTT es liviano y un puente publica luego en Kafka. Aquí `openmeteo_a_kafka.py` hace de puente entre la API y Kafka.

### S8 · Streaming con ventanas, watermark y semántica
| Concepto | En EcoData Aire |
|---|---|
| Ventana | Fija de 6 h: n, PM2.5 promedio y máximo |
| Watermark | 2 h: lo que llega más tarde se descarta y se cuenta (`filas_descartadas_watermark_total`) |
| Checkpoint | Uno por consulta (`_ckpt/ventanas`, `_ckpt/inferencia`) |
| Semántica | Al menos una vez; resultado idempotente porque el estado depende de la hora del dato |
| Evidencia | Publicar con `--tardio 30` sube el contador de descartadas |

### S9 · Observabilidad y costos
Ver [Observabilidad y alertas](observabilidad.md) y [Operación, escalado y costos](operacion.md).

### S10 · Series de tiempo e inferencia
- **Pregunta:** ¿se predice el PM2.5 de la próxima hora mejor que repitiendo la última hora?
- **Baseline primero:** la persistencia. La regresión lineal (MAE 0,742) no la superó (0,687); un GBT con pérdida absoluta sí (0,473, −31,1 %).
- **División cronológica**, nunca aleatoria (80/20; prueba desde el 20-oct-2023).
- **Misma función de features** en entrenamiento y servicio (`aire_comun.features`) para evitar *training-serving skew*.
- **En streaming:** modelo 0,422 frente a persistencia 0,567 (−25,5 %, 761 h).

### S11 · BI/ML y KPIs
- **KPI de negocio:** PM2.5 actual, predicción a +1 h, episodios, mejora frente a persistencia.
- **Métrica de infraestructura:** frescura, filas por segundo, descartes, mensajes por topic.
- **Métrica de aplicación:** MAE del modelo frente al de persistencia.
- **Batch y streaming lado a lado:** MAE de prueba del batch (U1) frente al MAE en vivo (U2).

## 2. Preguntas del jurado, con respuesta
**¿Diferencia entre un evento de sensor y uno de negocio?** El de sensor es periódico, de tamaño fijo y se valida por rango. El de negocio es irregular y nace de una regla (episodio); es el que dispara una acción.

**Si un evento llega tarde, ¿qué hace el watermark? ¿Y sin él?** Con watermark de 2 h, un evento con menos retraso entra en su ventana; con más, se descarta y se cuenta. Sin él, Spark guardaría el estado de todas las ventanas para siempre y no podría cerrar ninguna.

**¿Qué alertaría primero un backpressure?** La duración del micro-lote por encima del trigger de 5 s y la caída de filas por segundo; luego la frescura.

**¿Por qué comparar contra persistencia?** Porque «la próxima hora será igual que esta» es difícil de superar en series de tiempo. Si el modelo no la supera, no aporta. Nos pasó con la regresión lineal.

**¿Por qué usan Pushgateway?** Los jobs empujan métricas por lote; es un compromiso: un streaming no termina y, si muere, la métrica queda congelada. Lo cubrimos con el semáforo de frescura y la alerta de 10 min sin lote. La mejora sería un `/metrics` propio.

**¿Es en tiempo real?** El procesamiento sí; la fuente es horaria y modelada. Lo decimos abiertamente.

**¿Por qué cambiaron la pregunta?** Porque la norma peruana nunca se excedió en 2023: no había casos positivos. Pasamos a un pronóstico medible en todas las horas.

**¿Los números del tablero son reales?** El MAE sí es comparable. Los contadores (eventos, horas evaluadas, descartes) incluyen las reproducciones (*replay*) de la demostración, así que no equivalen a horas distintas de datos reales.

**¿Por qué hay más mensajes en Kafka que horas del año?** Además de 2023 se cargaron como histórico real los años 2024 y 2025 en topics propios, y la demostración reproduce datos; los topics acumulan más de 27 000 mensajes en total.

**¿Es exactly-once?** No: al menos una vez, con resultados idempotentes por la hora del dato.

**¿Escala?** Hoy es un nodo y un broker. Los criterios de escalado están en [Operación y costos](operacion.md).

**¿Por qué Lambda y no Kappa?** Para tener un histórico reproducible por lotes y un flujo continuo con el mismo modelo, y poder comparar el MAE de prueba del batch con el MAE en vivo.

## 3. Autoevaluación con la rúbrica
| Criterio | Peso | Evidencia | Estado |
|---|---|---|---|
| 1. Eventos empresariales con Kafka | 12 % | Kafka UI: `atmos-negocio`, contrato | A |
| 2. Eventos IoT/sensores | 10 % | Kafka UI: `atmos-aire`, esquema | A |
| 3. Streaming con Spark | 14 % | Ventana, watermark con `--tardio`, checkpoint | A |
| 4. Observabilidad | 12 % | Targets de Prometheus, 5 alertas, semáforo | A |
| 5. Costos y escalado | 8 % | [Operación y costos](operacion.md) | A |
| 6. Salidas BI/ML | 12 % | Tablero de Grafana | A |
| 7. Inferencia validada | 12 % | MAE modelo vs persistencia | A |
| 8. Sustentación | 20 % | Depende del dominio de este material | Practicar |

## 4. Decisiones propias
1. Datos reales de una API pública, no simulados.
2. Pregunta reformulada con evidencia (cero excesos del ECA).
3. Baseline de persistencia y modelo con pérdida absoluta.
4. Eventos de negocio (`aire.episodio`) en un topic aparte.
5. Honestidad de la fuente: modelada, ≈ 45 km, horaria.
6. Semáforo de frescura para suplir la limitación de Pushgateway.
7. Una sola función de features para batch y streaming.
