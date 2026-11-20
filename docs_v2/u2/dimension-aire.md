# Dimensión Aire — Unidad 2

## Pregunta de esta dimensión
¿El modelo sigue ganando a la persistencia **en vivo**, evento por evento, y el sistema se mantiene sano mientras lo hace?

## Pipeline en vivo
1. **Productor** (`openmeteo_a_kafka.py`): lee la serie de Open-Meteo y publica un evento JSON por hora, con `acks="all"` y clave `sensorId`. Modos: `--archivo` (replay con `--desde/--hasta/--pausa`), `--api` (últimas 48 h) y `--tardio HORAS` (evento atrasado para probar el watermark).
2. **Kafka**: topic `atmos-aire` para la telemetría y `atmos-negocio` para `aire.episodio`.
3. **Spark Structured Streaming** (`s12_streaming_aire.py`): lee con `subscribe`, `failOnDataLoss=false`, trigger de 5 s; dos consultas (ventanas e inferencia) comparten el flujo.
4. **Métricas**: un `StreamingQueryListener` empuja el avance de cada lote (descartes por watermark, filas en estado, watermark) al Pushgateway con el prefijo `lambda26_aire_`.
5. **Grafana**: el tablero `S12 – Calidad del aire` lee de Prometheus.

## Modelo
| Aspecto | Valor |
|---|---|
| Objetivo | Variación del PM2.5 a +1 h: `delta = PM2.5(t+1) − PM2.5(t)` |
| Algoritmo | `GBTRegressor`, pérdida absoluta, `maxIter=80`, `maxDepth=4`, `stepSize=0.1` |
| Features | 8, calculadas por **una sola función** (`aire_comun.features`) en entrenamiento y servicio |
| Regla de guardado | El modelo solo se guarda si gana a la persistencia |
| Prueba cronológica | **0,473 vs 0,687** (−31,1 %); la regresión lineal da 0,742 (peor) |
| En vivo | **0,422 vs 0,567** (−25,5 %), 761 h evaluadas, fuera de muestra |

## Ventanas, watermark y semántica
| Concepto | En EcoData Aire |
|---|---|
| Ventana | Fija de 6 h: cantidad de lecturas, PM2.5 promedio y máximo |
| Watermark | 2 h: lo que llega más tarde se descarta y se cuenta (`filas_descartadas_watermark_total`) |
| Checkpoint | Uno por consulta |
| Semántica | Al menos una vez; resultado idempotente por lote. No es exactly-once |
| Evidencia | Publicar con `--tardio 30` hace subir el contador de descartes |

## Eventos de negocio
Cuando PM2.5 supera el p95 de entrenamiento (10,5 µg/m³) se publica `aire.episodio` en `atmos-negocio`, separado de la telemetría. Es un evento irregular que nace de una regla y es el que dispara la acción del operador.

## Tablero de Grafana (16 paneles)
Último PM2.5 y predicción a +1 h, MAE del modelo vs persistencia, mejora %, horas evaluadas, episodios, semáforo de frescura, filas descartadas por watermark, estado de la consulta, mensajes totales y por topic en Kafka. Capturas en [Evidencias](evidencias.md).
