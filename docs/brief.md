# Brief técnico-analítico

*Plantilla del curso completada para la dimensión **Aire**. Solo se modifica si el alcance cambia de verdad. Las cifras son las medidas en las corridas del repositorio; los contadores de la demo incluyen reproducciones (replay) y no equivalen a horas distintas de datos reales.*

---

## 1. Datos del equipo

| Campo | Valor |
|---|---|
| Nombre del equipo | EcoData — dimensión Aire |
| Curso / sección | Big Data, sección G1, semestre 2026-2, UPeU campus Juliaca |
| Docente | Abel Ángel Sullón Macalupú |
| Repositorio | <https://github.com/Lanzeloth03/lambda26-bigdata> |
| Documentación publicada | Sitio MkDocs de este repositorio (`mkdocs.yml`) |
| Topics del repositorio | Lista en [Repositorio y estándares](repositorio.md). **Estado: configurar en GitHub** (incluye `grupo-<n>-ecodata-aire`) |
| Alcance de la entrega | Unidad 1 (batch) y Unidad 2 (streaming) |

| Integrante | Rol / énfasis |
|---|---|
| Lanzeloth Vargas Marichi | Extremo a extremo de la dimensión Aire: batch/Spark (Medallion), productor y streaming con Kafka, observabilidad (Prometheus + Grafana) y modelo predictivo |

---

## 2. Dominio del proyecto

| # | Elemento | Descripción |
|---|---|---|
| 1 | Nombre | EcoData Aire — alerta temprana de calidad del aire en Juliaca |
| 2 | Problema | Juliaca no tiene monitoreo accesible de material particulado. Se necesita anticipar el PM2.5 de la próxima hora y avisar de episodios para reducir la exposición |
| 3 | Dominio de datos | Ambiental / salud pública. PM2.5 y PM10 en µg/m³, una lectura por hora, punto (−15,5; −70,13) |
| 4 | Variable de interés | Concentración de PM2.5 en la hora siguiente |
| 5 | Pregunta central | ¿Un modelo entrenado con las últimas horas de PM2.5 y PM10 predice el PM2.5 de la próxima hora mejor que repetir la última hora (persistencia), tanto en batch como en vivo? |
| 6 | Usuarios y actores | **Operador / autoridad ambiental local** (recibe el aviso de episodio); **analista de datos** (consulta tendencias y MAE); **población sensible** (beneficiaria de la alerta); **docente-revisor** (evalúa el pipeline) |
| 7 | Arquitectura | **Lambda**: capa batch (histórico anual, Bronze→Silver→Gold) y capa de velocidad (eventos por Kafka + Spark Structured Streaming) con el **mismo modelo** y las mismas features. Ver [Arquitectura](arquitectura.md) |
| 8 | ¿Por qué Lambda y no Kappa? | El curso pide una capa batch con histórico reproducible (U1) y otra de flujo continuo (U2) comparables lado a lado. Con Lambda se mide el MAE de prueba del batch frente al MAE en vivo. Kappa reprocesaría todo desde el log y no permitiría esa comparación independiente |
| 9 | Fuente batch | Open-Meteo Air Quality (modelo CAMS), año 2023, 8 760 h (`aq_juliaca_2023.json`) |
| 10 | Fuente de eventos | La misma serie reproducida evento a evento hacia Kafka (`replay`), más la API en vivo cada hora. **Dato modelado (CAMS, rejilla ≈ 45 km), no una estación física: funciona como «sensor virtual»** |
| 11 | Nota sobre batch = streaming | El histórico de U1 es la acumulación del mismo flujo que alimenta U2; por eso ambas capas son comparables y no hay training-serving skew (`aire_comun.features`) |
| 12 | Continuidad | Dominio nuevo de este ciclo. Reutiliza la infraestructura Kafka/Spark/Grafana construida en S6–S11 |

---

## 3. Dimensión de análisis

### 3.1 Matriz de asignación

| Integrante | Dimensión | Indicador U1 | Indicador U2 | Fuente batch | Fuente streaming |
|---|---|---|---|---|---|
| Lanzeloth | Aire | MAE de PM2.5 a +1 h (GBT) vs persistencia; PM2.5 mensual y diario | MAE en vivo y mejora % vs persistencia; episodios; frescura | `aq_juliaca_2023.json` | topic `atmos-aire` |

### 3.2 Ficha de la dimensión

| Campo | Contenido |
|---|---|
| Tipo | Serie de tiempo horaria, regresión a +1 h |
| Dimensión como pregunta | ¿Cómo se comporta el PM2.5/PM10 en Juliaca y qué tan bien se anticipa el PM2.5 de la hora siguiente? |
| Decisión o acción que habilita | Avisar las horas con mayor probabilidad de episodio (PM2.5 > p95 de entrenamiento = 10,5 µg/m³) y priorizar reducción de exposición |
| Se combina con | Nada por ahora. Cruzar con temperatura o humedad es una mejora futura, fuera de alcance |

#### Unidad 1 — Batch

| # | Ítem | Contenido |
|---|---|---|
| 1 | Dimensión y relación con la pregunta | Aire es la variable de interés: se mide PM2.5/PM10 y se pronostica PM2.5 |
| 2 | Indicadores | MAE a +1 h; PM2.5 mensual y diario; patrón horario |
| 3 | Instrumento batch | PySpark: Bronze (JSON crudo) → Silver (limpieza, rangos físicos, nulos, duplicados) → Gold (Parquet **particionado por `mes`**, 12 particiones) |
| 4 | Modelo | 8 modelos comparados; gana **GBT de profundidad 3** (MLlib). División cronológica 80/20, nunca aleatoria; la prueba empieza el 20-oct-2023 |
| 5 | Resultado medido | MAE modelo **0,4635 µg/m³** vs persistencia **0,6871** (**−32,5 %**) |
| 6 | Salida | Notebooks S2–S4 e integrado; Gold Parquet; modelo guardado; figuras de MAE, mensual y diario |

#### Unidad 2 — Streaming

| # | Ítem | Contenido |
|---|---|---|
| 7 | Indicador | ¿El modelo sigue ganando a la persistencia en vivo? MAE a +1 h, mejora %, horas evaluadas, episodios, filas descartadas, frescura |
| 8 | Instrumento streaming | Open-Meteo → Kafka (`atmos-aire`, `atmos-negocio`) → Spark Structured Streaming: ventana fija de **6 h**, watermark **2 h**, trigger 5 s, checkpoint propio, inferencia en `foreachBatch` idempotente por `batchId` |
| 9 | Modelo | El mismo GBT con features de 24 h, comparado con persistencia cuando llega el dato real |
| 10 | Resultado medido | En vivo: **0,422 vs 0,567** (**−25,5 %**, 761 h). Integración: **0,4551 vs 0,6524** (**−30,2 %**, 143 h, 19/19 compuertas) |
| 11 | Salida | Tablero Grafana de 16 paneles (último PM2.5, predicción +1 h, MAE modelo vs persistencia, mejora %, semáforo de frescura, descartes por watermark, mensajes por topic) |
| 12 | Observabilidad | Pushgateway → Prometheus → Grafana; 5 reglas de alerta: `PushgatewayCaido`, `AireSinEventosNuevos`, `AireModeloNoSuperaPersistencia`, `DescartesPorWatermarkCreciendo`, `EpisodioDeAire` |

#### Requisitos iniciales

1. Publicar cada lectura horaria como evento `aire.lectura` en Kafka, con clave `sensorId` para conservar el orden.
2. Validar esquema y rango físico (PM ≥ 0) antes de procesar.
3. Pronosticar el PM2.5 de la próxima hora y compararlo con la persistencia cuando llegue el dato real.
4. Descartar y **contar** los datos tardíos fuera de 2 h de watermark.
5. Publicar `aire.episodio` en `atmos-negocio` cuando PM2.5 supere el p95 de entrenamiento.
6. Reanudar desde checkpoint tras un reinicio sin duplicar métricas.
7. Bloquear la ejecución integrada si falla una compuerta de calidad y dejar un manifiesto.
8. Declarar abiertamente que la fuente es modelada y horaria.

### 3.3 Alcance en conjunto

| Sí cubre | No cubre |
|---|---|
| Pipeline batch + streaming de PM2.5/PM10 de un punto de Juliaca | Red de estaciones |
| Pronóstico a +1 h con baseline de persistencia | Pronóstico a más de 1 h |
| Eventos de sensor (`aire.lectura`) y de negocio (`aire.episodio`) | Otros contaminantes (O₃, NO₂) |
| Observabilidad con alertas y semáforo de frescura | Validación contra mediciones de una estación real |
| Integración reproducible con compuertas y manifiesto | Alerta normativa: el ECA nunca se excedió en 2023, por eso la pregunta es un pronóstico y no una alerta de norma |
| Costo y escalado de referencia | Exactly-once de punta a punta (se ofrece *al menos una vez* con resultados idempotentes) |
| | Despliegue en la nube |

**Costo de referencia.** Entorno actual (laptop, Docker): US$ 0. Plantilla del curso 3 × t3.medium: US$ 91,15/mes. Alternativa ajustada al volumen, 1 × t3.medium: ≈ US$ 30,4/mes. Son cifras de referencia a verificar con la calculadora del proveedor. Detalle en [Observabilidad y costos](observabilidad-costos.md).

**Pendiente para siguientes sesiones.** Volumen persistente para Kafka, `/metrics` propio en lugar de Pushgateway.

---

## 4. Aprobación

| Docente | Fecha | Visto |
|---|---|---|
| Abel Ángel Sullón Macalupú | *pendiente* | *pendiente de visto del docente* |
