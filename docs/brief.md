# Brief Técnico-Analítico del Proyecto Sello: EcoData

Hito S2 del cronograma. Fija qué sistema Big Data construye el equipo durante el ciclo, antes de avanzar a S3. Se actualiza solo si el alcance cambia de verdad.

## 1. Datos del equipo

- **Nombre del equipo:** EcoData
- **Sección:** [completar]
- **Repositorio (URL):** [https://github.com/Alahinticona/ecodata-juliaca-u1](https://github.com/Alahinticona/ecodata-juliaca-u1)
- **Documentación publicada:** [https://alahinticona.github.io/ecodata-juliaca-u1/](https://alahinticona.github.io/ecodata-juliaca-u1/)
- **Topics del repositorio:** a configurar por el propietario del repositorio: `big-data`, `juliaca`, `lambda-architecture`, `open-meteo`, `pyspark`, `spark-mllib`, y para U2 `kafka`, `spark-streaming`, `grafana`, `prometheus`, `streaming`. Falta el topic con el formato `grupo-<numero>-ecodata`: [completar el número de grupo].

**Integrantes:**

| Integrante | Rol o énfasis previsto |
|---|---|
| Alahin Reyme Ticona Veliz | Dimensión clima. Énfasis en ingesta de eventos y modelado de series de tiempo (batch con Spark y streaming con Kafka) |
| Lanzeloth Vargas Marichi | Dimensión aire. Énfasis en observabilidad (Grafana, Prometheus) y documentación |

Este rol es un énfasis de coordinación a nivel de equipo, no un reemplazo del trabajo individual: cada integrante construye de extremo a extremo sus dos dimensiones, U1 y U2.

## 2. Dominio del proyecto

- **Nombre del proyecto:** EcoData: Sistema de Alerta Temprana de Calidad del Aire y Clima en Juliaca.
- **Problema o necesidad que resuelve:** Juliaca, a más de 3 800 m de altitud, sufre heladas nocturnas y episodios de aire cargado de partículas, y la población no recibe un aviso anticipado. El sistema procesa las lecturas ambientales en bloque (histórico) y en vivo (cada hora), y pronostica la hora siguiente para anticipar el riesgo.
- **Dominio de datos:** ambiental y meteorológico (sector público y salud ambiental). Se registran temperatura, humedad, viento, PM2.5 y PM10 por hora.
- **Pregunta central de negocio:** ¿cómo anticipar las condiciones de riesgo ambiental (helada y mala calidad del aire) en Juliaca para emitir una alerta temprana?
- **Usuarios y actores principales:** población y productores agropecuarios de Juliaca, autoridades de salud ambiental y de gestión de riesgos, y el sistema de alertas (topic `atmos-negocio`).
- **Arquitectura Big Data prevista:** **Lambda.** La misma fuente alimenta la capa batch (U1: limpieza, Parquet particionado y modelo entrenado una vez) y la capa de velocidad (U2: Kafka y Spark Structured Streaming con inferencia en cada evento). Se eligió Lambda y no Kappa porque el histórico se reprocesa por lotes para entrenar y el flujo en vivo necesita baja latencia; ambos usan el mismo origen.
- **Fuente de datos batch:** datos reales de Open-Meteo (API abierta, reanálisis y modelo CAMS para aire) de Juliaca: `juliaca_clima_historico.csv` y `juliaca_calidad_aire.csv`, guardados como Parquet particionado.
- **Fuente de eventos en tiempo real:** las mismas variables de Open-Meteo, publicadas hora por hora a Kafka por un productor (`openmeteo_clima_a_kafka.py`, `openmeteo_a_kafka.py`) en los topics `atmos-clima` y `atmos-aire`. Los datos son modelados, no de un sensor propio; el ESP32 con HC-SR04 queda como fuente secundaria.
- **¿Continúa un proyecto anterior?** No. Es un dominio nuevo.

La fuente batch y la de streaming son el mismo origen visto en dos momentos: el histórico acumulado y la lectura en vivo.

## 3. Dimensiones de análisis y fuentes previstas

Cada integrante aporta dos dimensiones de la misma pregunta central, y todas convergen en un solo tablero de Grafana.

**Tabla de asignación:**

| Integrante | Tipo | Dimensión (como pregunta) | Indicador | Fuente batch | Fuente streaming |
|---|---|---|---|---|---|
| Alahin | Predictiva (U1, batch) | ¿Cuál es la temperatura de Juliaca y cuál se puede esperar según el histórico? | Temperatura media y mínima por mes; temperatura estimada por RandomForest | Histórico de temperatura de Open-Meteo | (mismo origen, acumulado; no se procesa en vivo) |
| Alahin | Predictiva (U2, streaming) | ¿Cuál será la temperatura de la próxima hora y habrá helada? | Temperatura pronosticada en °C a +1 h; evento `clima.helada` | (mismo histórico, para entrenar) | Eventos horarios de Open-Meteo en `atmos-clima` |
| Lanzeloth | Predictiva (U1, batch) | ¿Cómo ha sido la calidad del aire (PM2.5 y PM10) y qué riesgo se puede esperar? | Concentración media de PM2.5 y PM10; riesgo estimado por el modelo de U1 | Histórico de calidad del aire de Open-Meteo (CAMS) | (mismo origen, acumulado; no se procesa en vivo) |
| Lanzeloth | Predictiva (U2, streaming) | ¿Cuál será el PM2.5 de la próxima hora y se entrará en episodio? | PM2.5 pronosticado en µg/m³ a +1 h; evento `aire.episodio` | (mismo histórico, para entrenar) | Eventos horarios de Open-Meteo en `atmos-aire` |

Los paneles de las dos dimensiones U2 se muestran en el mismo Grafana (`S12 - EcoData U2: aire + clima`); las dimensiones U1 se documentan en sus notebooks.

### Dimensión: Temperatura, ventana batch (integrante: Alahin Reyme Ticona Veliz)

- **Tipo:** predictiva sin tiempo real (U1, batch).
- **Dimensión:** ¿cuál es la temperatura de Juliaca y cuál se puede esperar a partir del histórico? Aporta el nivel de frío que alimenta la pregunta central de riesgo.
- **Indicadores:** temperatura media y mínima por mes; temperatura estimada por el modelo (RMSE 1,4351 °C).
- **Decisión o acción que habilita:** identificar las épocas y horas de mayor riesgo de helada para planificar la protección.
- **Instrumento batch:** `juliaca_clima_historico.csv` (Open-Meteo, por hora; temperatura, humedad y viento), guardado como Parquet particionado por año y mes.
- **Instrumento streaming:** no aplica en vivo; es el mismo origen de U2, acumulado.
- **Modelo predictivo:** regresión con RandomForest (Spark MLlib), entrenado una sola vez.
- **Salida:** notebook `01_pipeline_completo_u1.ipynb` con tablas, gráficos y la predicción.
- **Se combina con:** la dimensión de temperatura en vivo y la dimensión de aire en el tablero final.
- **Requisitos iniciales:** 1) el sistema debe limpiar y particionar el histórico en Parquet; 2) el sistema debe entrenar y evaluar un modelo de temperatura; 3) el sistema debe reportar el error del modelo en el notebook.

### Dimensión: Temperatura, ventana en vivo (integrante: Alahin Reyme Ticona Veliz)

- **Tipo:** predictiva con tiempo real (U2, streaming).
- **Dimensión:** ¿cuál será la temperatura de la próxima hora y habrá helada? Responde a la pregunta central con el estado de frío en vivo.
- **Indicadores:** temperatura pronosticada a +1 h; error medio absoluto (MAE) del modelo contra la persistencia (0,536 °C contra 0,982 °C, −45,5 % en 757 horas en vivo; en prueba 0,537 contra 1,096 °C, −51,0 %); heladas detectadas.
- **Decisión o acción que habilita:** publicar `clima.helada` en `atmos-negocio` cuando la temperatura baja de 0 °C, para alertar a quien debe proteger cultivos.
- **Instrumento batch:** el mismo histórico, para entrenar.
- **Instrumento streaming:** topic `atmos-clima` en Kafka (esquema: `tipoEvento`, `sensorId`, `temp`, `hum`, `viento`, `origen`, `timestamp`), alimentado por Open-Meteo.
- **Modelo predictivo:** GBTRegressor sobre el cambio de temperatura a una hora, aplicado en vivo con Spark Structured Streaming (ventana de 6 h, watermark de 2 h, checkpoint).
- **Salida:** sección «Dimensión Clima» del Grafana común, con temperatura real contra pronóstico, MAE contra persistencia y descartes del watermark.
- **Se combina con:** la dimensión de aire en el mismo tablero.
- **Requisitos iniciales:** 1) el sistema debe ingerir eventos de temperatura por Kafka con un contrato de esquema; 2) el sistema debe pronosticar la hora siguiente y medir su error contra la persistencia; 3) el sistema debe descartar los datos tardíos con watermark y exponer ese contador en Grafana.

### Dimensión: Calidad del aire, ventana batch (integrante: Lanzeloth Vargas Marichi)

- **Tipo:** predictiva sin tiempo real (U1, batch).
- **Dimensión:** ¿cómo ha sido la concentración de PM2.5 y PM10 en Juliaca y qué riesgo se puede esperar? Aporta el nivel de contaminación del aire a la pregunta central.
- **Indicadores:** concentración media de PM2.5 y PM10 por periodo; PM2.5 pronosticado a +1 h con regresión lineal de Spark MLlib (RMSE 1,891 y R² 0,891 sobre 464 horas de prueba; persistencia: RMSE 1,944).
- **Decisión o acción que habilita:** identificar los periodos de aire más cargado y orientar las recomendaciones a la población.
- **Instrumento batch:** `juliaca_calidad_aire.csv` (Open-Meteo, modelo CAMS, por hora; PM2.5 y PM10), guardado como Parquet particionado.
- **Instrumento streaming:** no aplica en vivo; es el mismo origen de U2, acumulado.
- **Modelo predictivo:** regresión de PM2.5 a +1 h con Spark MLlib (se compararon regresión lineal, RandomForest y GBT frente a la persistencia), entrenada una sola vez; el nivel de riesgo (Bajo, Moderado, Alto) se deriva de los umbrales 25 y 50 µg/m³.
- **Salida:** script `s12/entrena_aire_u1.py` y métricas en `s12/aire_u1_metricas.json`, con la documentación en la página «Dimensión Aire (Lanzeloth)».
- **Se combina con:** la dimensión de aire en vivo y la dimensión de clima en el tablero final.
- **Requisitos iniciales:** 1) el sistema debe limpiar la calidad del aire y guardarla en Parquet; 2) el sistema debe entrenar y evaluar un modelo de riesgo; 3) el sistema debe reportar sus métricas en el notebook.

### Dimensión: Calidad del aire, ventana en vivo (integrante: Lanzeloth Vargas Marichi)

- **Tipo:** predictiva con tiempo real (U2, streaming).
- **Dimensión:** ¿cuál será el PM2.5 de la próxima hora y se entrará en episodio? Responde a la pregunta central con el estado del aire en vivo. La pregunta original («¿cuándo se superará el umbral?») se reformuló: en 2023 el PM2.5 nunca superó la norma ECA (p95 ≈ 10,5 µg/m³), así que no había casos positivos que medir.
- **Indicadores:** PM2.5 pronosticado a +1 h; MAE del modelo contra la persistencia (0,422 contra 0,567 µg/m³, −25,5 % en 761 horas en vivo; en prueba 0,473 contra 0,687, −31,1 %); episodios detectados.
- **Decisión o acción que habilita:** publicar `aire.episodio` en `atmos-negocio` cuando el PM2.5 supera el p95 de entrenamiento, para activar un aviso.
- **Instrumento batch:** el mismo histórico, para entrenar.
- **Instrumento streaming:** topic `atmos-aire` en Kafka, alimentado por Open-Meteo (CAMS).
- **Modelo predictivo:** GBTRegressor sobre el cambio de PM2.5 a una hora, aplicado en vivo con Spark Structured Streaming. La regresión lineal se descartó porque pierde contra la persistencia (0,742).
- **Salida:** sección «Dimensión Aire» del Grafana común, con PM2.5 real contra pronóstico, MAE contra persistencia, eventos y descartes del watermark.
- **Se combina con:** la dimensión de clima en el mismo tablero.
- **Requisitos iniciales:** 1) el sistema debe ingerir eventos de PM2.5 y PM10 por Kafka; 2) el sistema debe pronosticar la hora siguiente y publicar episodios en `atmos-negocio`; 3) el sistema debe exponer en Grafana el error del modelo, el volumen y los descartes.

### Alcance del proyecto en conjunto

- **Qué SÍ cubre:** ingesta batch y en vivo desde Open-Meteo, procesamiento con Spark (batch y Structured Streaming), pronóstico a una hora de temperatura y PM2.5, eventos de negocio (`clima.helada`, `aire.episodio`), tablero único en Grafana, costo de referencia (US$ 91,15 al mes con 3 × t3.medium) y documentación publicada.
- **Qué NO cubre (fuera de alcance):** sensores propios en producción (el ESP32 es secundario); pronóstico a más de una hora; garantía exactly-once de punta a punta; varias estaciones o ubicaciones; alerta conjunta de clima y aire; despliegue en la nube y empaquetado DataOps (se trabajan en U3).

**Pendiente para las siguientes sesiones:** la infraestructura compartida (Spark, Kafka, almacenamiento analítico, observabilidad con Grafana y estimación de costos operacionales, prácticas de DataOps) sigue el patrón que enseña cada sesión sobre `lambda26`; se construye en clase, sesión por sesión, sobre las dimensiones y fuentes de esta ficha.

## 4. Aprobación

- **Docente:** [completar]
- **Fecha:** [completar]
