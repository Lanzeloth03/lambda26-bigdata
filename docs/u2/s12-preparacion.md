# Preparación S12: evaluación de la Unidad II

Cada tema de S6 a S11 respondido con **nuestro** pipeline EcoData (aire, Juliaca).

## 1. Teoría S6 a S11 con ejemplos de EcoData

### S6 · Eventos empresariales con Kafka
| Concepto | En EcoData |
|---|---|
| Broker | Un broker Kafka (contenedor `lambda26-kafka`), puerto 49092 |
| Topic | `atmos-aire` (telemetría) y `atmos-negocio` (eventos de negocio) |
| Partición y key | 1 partición por topic; key = `sensorId`, así los eventos de una ubicación conservan su orden |
| Offset | Posición del mensaje; Spark guarda el último offset leído en su checkpoint |
| Contrato | JSON con esquema versionado en `schemas/*.json` |
| Evento empresarial | `aire.episodio`: nace de una regla de negocio, no de la fuente |

### S7 · Eventos IoT/sensores
- **Frecuencia y volumen:** 1 lectura por hora (dato modelado de Open-Meteo, tamaño constante ≈ 0,17 KB).
- **Validación:** esquema y rango físico (PM ≥ 0).
- **Fuente:** dato modelado de Open-Meteo (CAMS, ~45 km), horario; funciona como «sensor virtual».
- **¿Por qué un sensor real usa MQTT y no Kafka?** Un sensor tiene poca memoria, batería y conexión intermitente. MQTT es liviano; un puente publica luego en Kafka. Aquí `openmeteo_a_kafka.py` hace de puente entre la API y Kafka.

### S8 · Streaming con ventanas, watermark y semántica
| Concepto | En EcoData |
|---|---|
| Ventana | Fija de **6 horas** (`ventanas`): n, PM2.5 promedio y máximo |
| Watermark | **2 horas**: lo que llega más tarde se descarta y se cuenta (`filas_descartadas_watermark_total`) |
| Checkpoint | Uno por consulta en `/tmp/s12/aire/_ckpt/...` |
| Semántica | Al menos una vez; resultado idempotente porque el estado depende de la hora del dato |
| Evidencia | Publicar con `--tardio 30`: sube el contador de descartadas |

### S9 · Observabilidad y costos
Ver [Observabilidad y alertas](observabilidad-alertas.md) y [Operación, escalado y costos](operacion.md).

### S10 · Series de tiempo e inferencia
- **Pregunta:** ¿se predice el PM2.5 de la próxima hora mejor que repetir la última hora?
- **Baseline primero:** la persistencia. La regresión lineal (MAE 0,742) **no** la superó (0,687); un GBT con pérdida absoluta sí (0,473, −31,1 %).
- **División cronológica**, nunca aleatoria (80/20; prueba desde el 20-oct-2023).
- **Misma función de features en entrenamiento y servicio** (`aire_comun.features`), para evitar training-serving skew.
- **En streaming:** modelo 0,43–0,45 frente a persistencia 0,56–0,58 (≈ 20–25 % de mejora) en aire.

### S11 · BI/ML y KPIs
- **KPI de negocio:** PM2.5 actual, predicción +1 h, episodios, mejora frente a persistencia.
- **Métrica de infraestructura:** frescura, filas por segundo, descartes.
- **Métrica de aplicación:** MAE del modelo frente al de persistencia.
- **Batch y streaming lado a lado:** MAE de prueba del batch (U1) frente al MAE en vivo.

## 2. Preguntas del jurado, con respuesta
**¿Diferencia entre un evento de sensor y uno de negocio?**
El de sensor es periódico, de tamaño fijo y se valida por rango. El de negocio es irregular y nace de una regla (episodio); es el que dispara una acción.

**Si un evento llega tarde, ¿qué hace el watermark? ¿Y sin él?**
Con watermark de 2 h, un evento con menos retraso entra en su ventana; con más, se descarta y se cuenta. Sin él, Spark guardaría el estado de todas las ventanas para siempre y no podría cerrar ninguna.

**¿Qué alertaría primero un backpressure?**
La duración del micro-lote por encima del trigger de 5 s y la caída de filas por segundo; luego la frescura.

**¿Por qué comparar contra persistencia?**
Porque «la próxima hora será igual que esta» es difícil de superar en series de tiempo. Si el modelo no la supera, no aporta. A nosotros nos pasó con la regresión lineal.

**¿Por qué usan Pushgateway?**
Los jobs empujan métricas por lote. Es un compromiso: un streaming no termina, y si muere la métrica queda congelada. Lo cubrimos con el semáforo de frescura y la alerta de 10 min sin lote. La mejora sería un `/metrics` propio.

**¿Es en tiempo real?**
El procesamiento sí; la fuente es horaria y modelada. Lo decimos abiertamente.

**¿Por qué cambiaron la pregunta?**
Porque la norma peruana nunca se excedió en 2023: no había casos positivos. Pasamos a un pronóstico medible en todas las horas.

**¿El modelo sirve fuera de 2023?**
Sí. Sin reentrenar, en datos reales posteriores gana a la persistencia: −32,9 % en 2024, −21,3 % en 2025 y −24,1 % en 2026, y en 34 de los 35 meses evaluados. La única excepción es noviembre de 2025. La ventaja baja en 2025, por eso se propone reentrenar periódicamente. Detalle en [Validación 2024-2026](validacion-2024-2026.md).

**¿Hay datos de 2026?**
Sí: la API entrega la serie horaria desde el 01/01/2026; se descargó hasta el 20/11/2026 y se usó solo para validar.

**¿Por qué hay más mensajes en Kafka que horas de 2023?**
Además de 2023 se cargaron los años 2024 y 2025 como histórico real, cada uno en su topic (`atmos-aire-hist-2024` y `atmos-aire-hist-2025`), y el demo y las corridas de integración tienen sus propios topics. En total el broker acumula alrededor de 27 000 mensajes.

**¿Los números del tablero son reales?**
El MAE sí es comparable. Los contadores (eventos, horas, descartadas) están inflados por los replays de la demo.

**¿Es exactamente una vez?**
No: al menos una vez, con resultados idempotentes por la hora del dato.

**¿Escala?**
Hoy es un nodo y un broker. Los criterios de escalado están en [operacion.md](operacion.md).

## 3. Autoevaluación con la rúbrica
| Criterio | Peso | Evidencia | Estado |
|---|---:|---|---|
| 1. Eventos empresariales con Kafka | 12 % | Kafka UI `atmos-negocio`, contrato v1 | A |
| 2. Eventos IoT/sensores | 10 % | Kafka UI `atmos-aire`, esquemas | A |
| 3. Streaming con Spark | 14 % | Notebook 6, watermark con `--tardio`, checkpoint | A |
| 4. Observabilidad | 12 % | Prometheus targets y alertas, semáforo | A (tras activar `alerts.yml`) |
| 5. Costos y escalado | 8 % | [operacion.md](operacion.md) | A |
| 6. Salidas BI/ML | 12 % | Dashboard Grafana | A |
| 7. Inferencia validada | 12 % | MAE modelo vs persistencia | A |
| 8. Sustentación | 20 % | Este material y la demo ([Puesta en marcha](puesta-en-marcha.md)) | **Practicar** |

## 4. Decisiones propias
1. Datos reales de una API pública, no simulados.
2. Una sola función de features para batch y streaming.
3. Pregunta reformulada con evidencia (cero excesos del ECA).
4. Baseline de persistencia y modelo con pérdida absoluta.
5. Eventos de negocio (`aire.episodio`) en un topic aparte.
6. Fuente consumida por API cada hora (CAMS, ~45 km).
7. Semáforo de frescura para suplir la limitación de Pushgateway.
