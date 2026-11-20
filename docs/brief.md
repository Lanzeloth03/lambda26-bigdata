# Brief Técnico-Analítico del Proyecto Sello: EcoData

Hito S2 del cronograma. Fija qué sistema Big Data construye el equipo durante el ciclo, antes de avanzar a S3. Se actualiza solo si el alcance cambia de verdad.

## 1. Datos del equipo

- **Nombre del equipo:** EcoData (dimensión Aire)
- **Sección:** G1
- **Repositorio (URL):** [https://github.com/Lanzeloth03/lambda26-bigdata](https://github.com/Lanzeloth03/lambda26-bigdata)
- **Documentación publicada:** [https://lanzeloth03.github.io/lambda26-bigdata/](https://lanzeloth03.github.io/lambda26-bigdata/)
- **Topics del repositorio:** a configurar en GitHub: `big-data`, `juliaca`, `lambda-architecture`, `open-meteo`, `pyspark`, `spark-mllib`, y para U2 `kafka`, `spark-streaming`, `grafana`, `prometheus`, `streaming`. Falta el topic con el formato `grupo-<numero>-ecodata-aire`.

**Integrantes:**

| Integrante | Rol o énfasis previsto |
|---|---|
| Lanzeloth Vargas Marichi | Dimensión aire, de extremo a extremo: batch con Spark, streaming con Kafka, observabilidad (Grafana, Prometheus) y documentación |

El integrante construye de extremo a extremo la dimensión Aire, en U1 (batch) y U2 (streaming).

## 2. Dominio del proyecto

- **Nombre del proyecto:** EcoData: Sistema de Alerta Temprana de Calidad del Aire en Juliaca.
- **Problema o necesidad que resuelve:** Juliaca, a más de 3 800 m de altitud, sufre episodios de aire cargado de partículas y la población no recibe un aviso anticipado. El sistema procesa las lecturas de calidad del aire en bloque (histórico) y en vivo (cada hora), y pronostica la hora siguiente para anticipar el riesgo.
- **Dominio de datos:** ambiental (sector público y salud ambiental). Se registran PM2.5 y PM10 por hora.
- **Pregunta central de negocio:** ¿cómo anticipar los episodios de mala calidad del aire en Juliaca para emitir una alerta temprana?
- **Usuarios y actores principales:** población de Juliaca, autoridades de salud ambiental y de gestión de riesgos, y el sistema de alertas (topic `atmos-negocio`).
- **Arquitectura Big Data prevista:** **Lambda.** La misma fuente alimenta la capa batch (U1: limpieza, Parquet particionado y modelo entrenado una vez) y la capa de velocidad (U2: Kafka y Spark Structured Streaming con inferencia en cada evento). Se eligió Lambda y no Kappa porque el histórico se reprocesa por lotes para entrenar y el flujo en vivo necesita baja latencia; ambos usan el mismo origen.
- **Fuente de datos batch:** datos reales de Open-Meteo (API abierta, modelo CAMS para aire) de Juliaca. El año **2023** completo (8 760 horas, `aq_juliaca_2023.json`) alimenta el pipeline batch (Bronze → Silver → Gold en Parquet particionado por mes). Los años **2024 y 2025** se descargaron de la misma fuente, se cargaron a Kafka como histórico real y, junto con 2026 (hasta el 20/11), se usaron para validar el modelo fuera de muestra (MAE −32,9 %, −21,3 % y −24,1 % frente a la persistencia).
- **Fuente de eventos en tiempo real:** las mismas variables de Open-Meteo, publicadas hora por hora a Kafka por un productor (`openmeteo_a_kafka.py`) en los topics `atmos-aire` (telemetría) y `atmos-negocio` (eventos de negocio), más los topics de demostración e histórico (`atmos-aire-demo`, `atmos-aire-hist-2024`, `atmos-aire-hist-2025`). Los datos son modelados (CAMS, ≈ 45 km), no de un sensor propio.
- **¿Continúa un proyecto anterior?** No. Es un dominio nuevo.

La fuente batch y la de streaming son el mismo origen visto en dos momentos: el histórico acumulado y la lectura en vivo.

## 3. Dimensiones de análisis y fuentes previstas

La dimensión Aire se aborda en dos ventanas de la misma pregunta central (U1 batch y U2 streaming) y converge en un tablero de Grafana.

**Tabla de asignación:**

| Integrante | Tipo | Dimensión (como pregunta) | Indicador | Fuente batch | Fuente streaming |
|---|---|---|---|---|---|
| Lanzeloth | Predictiva (U1, batch) | ¿Cómo ha sido la calidad del aire (PM2.5 y PM10) y qué riesgo se puede esperar? | Concentración media de PM2.5 y PM10; PM2.5 pronosticado a +1 h por el modelo de U1 | Histórico de calidad del aire de Open-Meteo (CAMS) | (mismo origen, acumulado; no se procesa en vivo) |
| Lanzeloth | Predictiva (U2, streaming) | ¿Cuál será el PM2.5 de la próxima hora y se entrará en episodio? | PM2.5 pronosticado en µg/m³ a +1 h; evento `aire.episodio` | (mismo histórico, para entrenar) | Eventos horarios de Open-Meteo en `atmos-aire` |

Los paneles de la dimensión U2 se muestran en Grafana (`S12 - Calidad del aire`); la dimensión U1 se documenta en sus notebooks.

### Dimensión: Calidad del aire, ventana batch (integrante: Lanzeloth Vargas Marichi)

- **Tipo:** predictiva sin tiempo real (U1, batch).
- **Dimensión:** ¿cómo ha sido la concentración de PM2.5 y PM10 en Juliaca y qué riesgo se puede esperar? Aporta el nivel de contaminación del aire a la pregunta central.
- **Indicadores:** concentración media de PM2.5 y PM10 por mes y por día; PM2.5 pronosticado a +1 h y MAE del modelo contra la persistencia (**0,4635 contra 0,6871 µg/m³, −32,5 %** sobre 1 747 horas de prueba, R² 0,9261).
- **Decisión o acción que habilita:** identificar los periodos de aire más cargado y orientar las recomendaciones a la población.
- **Instrumento batch:** `aq_juliaca_2023.json` (Open-Meteo, modelo CAMS, por hora; PM2.5 y PM10; 8 760 filas), procesado con PySpark (Bronze → Silver → Gold) y guardado como Parquet particionado por `mes` (12 particiones).
- **Instrumento streaming:** no aplica en vivo; es el mismo origen de U2, acumulado.
- **Modelo predictivo:** regresión de la variación de PM2.5 a +1 h con Spark MLlib. Se compararon 8 modelos (persistencia, regresión lineal, Ridge, Elastic Net, Random Forest y GBT de profundidad 3, 4 y 5) con partición cronológica 80/20; gana el **GBT de profundidad 3**. El nivel de riesgo (Bajo, Moderado, Alto) se deriva de los umbrales 25 y 50 µg/m³.
- **Salida:** cuadernos `S2_aire_pyspark`, `S3_aire_calidad_parquet`, `S4_aire_modelo_mllib` y `u1_aire_lanzeloth` (integrado), Gold en Parquet, modelo guardado y figuras de MAE, PM2.5 mensual y patrón diario; documentación en «Dimensión Aire ampliada».
- **Se combina con:** la dimensión de aire en vivo en el tablero final.
- **Requisitos iniciales:** 1) el sistema debe limpiar la calidad del aire y guardarla en Parquet; 2) el sistema debe entrenar y comparar modelos contra la persistencia; 3) el sistema debe reportar sus métricas en el notebook.

### Dimensión: Calidad del aire, ventana en vivo (integrante: Lanzeloth Vargas Marichi)

- **Tipo:** predictiva con tiempo real (U2, streaming).
- **Dimensión:** ¿cuál será el PM2.5 de la próxima hora y se entrará en episodio? Responde a la pregunta central con el estado del aire en vivo. La pregunta original («¿cuándo se superará el umbral?») se reformuló: en 2023 el PM2.5 nunca superó la norma ECA (p95 ≈ 10,5 µg/m³), así que no había casos positivos que medir.
- **Indicadores:** PM2.5 pronosticado a +1 h; MAE del modelo contra la persistencia (0,422 contra 0,567 µg/m³, −25,5 % en 761 horas en vivo; en prueba 0,473 contra 0,687, −31,1 %); episodios detectados.
- **Decisión o acción que habilita:** publicar `aire.episodio` en `atmos-negocio` cuando el PM2.5 supera el p95 de entrenamiento, para activar un aviso.
- **Instrumento batch:** el mismo histórico, para entrenar.
- **Instrumento streaming:** topic `atmos-aire` en Kafka, alimentado por Open-Meteo (CAMS); ventana de 6 h, watermark de 2 h, checkpoint por consulta e inferencia idempotente por lote.
- **Modelo predictivo:** GBTRegressor sobre el cambio de PM2.5 a una hora, aplicado en vivo con Spark Structured Streaming. La regresión lineal se descartó porque pierde contra la persistencia (0,742).
- **Salida:** tablero de Grafana `S12 - Calidad del aire` (16 paneles): PM2.5 real contra pronóstico, MAE contra persistencia, mejora %, episodios, semáforo de frescura, descartes del watermark y mensajes por topic; 5 reglas de alerta en Prometheus.
- **Se combina con:** la ventana batch de aire (mismo modelo y mismas features).
- **Requisitos iniciales:** 1) el sistema debe ingerir eventos de PM2.5 y PM10 por Kafka; 2) el sistema debe pronosticar la hora siguiente y publicar episodios en `atmos-negocio`; 3) el sistema debe exponer en Grafana el error del modelo, el volumen y los descartes.

### Alcance del proyecto en conjunto

- **Qué SÍ cubre:** ingesta batch (2023) y en vivo desde Open-Meteo, histórico real de 2024 y 2025 en Kafka, procesamiento con Spark (batch y Structured Streaming), pronóstico a una hora de PM2.5, evento de negocio (`aire.episodio`), observabilidad con Prometheus y Grafana (5 alertas), integración reproducible con 19 compuertas y manifiesto, y costo de referencia (US$ 91,15 al mes con 3 × t3.medium en la plantilla; ≈ US$ 30,4 con una sola máquina).
- **Qué NO cubre (fuera de alcance):** sensores propios en producción (el ESP32 es secundario); pronóstico a más de una hora; garantía exactly-once de punta a punta; varias estaciones o ubicaciones; despliegue en la nube y empaquetado DataOps (se trabajan en U3).

**Pendiente:** volumen persistente para Kafka, exportador `/metrics` propio en lugar de Pushgateway y reentrenamiento programado con detección de deriva.

## 4. Aprobación

- **Docente:** Abel Ángel Sullón Macalupú
- **Fecha:** pendiente
- **Visto del docente:** pendiente
