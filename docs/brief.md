# Brief técnico-analítico

*Plantilla del curso, completada para la dimensión Aire. Se actualiza solo si el alcance cambia de verdad.*

---

## 1. Datos del equipo

| Campo | Valor |
|---|---|
| Nombre del equipo | EcoData (dimensión Aire) |
| Sección | G1 — Big Data, semestre 2026-2, campus Juliaca |
| Repositorio | <https://github.com/Lanzeloth03/lambda26-bigdata> |
| Topics | Lista en [Repositorio y estándares](repositorio.md). **Estado: configurar en GitHub** (incluye `grupo-<n>-ecodata-aire`) |

| Integrante | Rol / énfasis |
|---|---|
| Lanzeloth Vargas Marichi | Batch/Spark, streaming/Kafka, observabilidad y ML de la dimensión Aire (extremo a extremo) |

---

## 2. Dominio del proyecto

| # | Elemento | Descripción |
|---|---|---|
| 1 | Nombre | EcoData Aire — alerta temprana de calidad del aire en Juliaca |
| 2 | Problema | Juliaca no cuenta con monitoreo accesible de material particulado; se necesita anticipar el PM2.5 de la próxima hora y avisar de episodios |
| 3 | Dominio de datos | Ambiental / salud pública; PM2.5 y PM10 (µg/m³) cada hora |
| 4 | Variable de interés | Concentración de PM2.5 |
| 5 | Pregunta central | ¿Un modelo entrenado con las últimas horas de PM2.5 y PM10 predice el PM2.5 de la próxima hora mejor que repetir la última hora? |
| 6 | Usuarios | Operador/autoridad ambiental local, analista, docente-revisor |
| 7 | Arquitectura | **Lambda**: histórico anual por lotes + flujo continuo por eventos con el mismo modelo (ver [Arquitectura](arquitectura.md)) |
| 8 | Fuente batch | Open-Meteo Air Quality (CAMS), año 2023, 8 760 h, Juliaca (−15,5; −70,13) |
| 9 | Fuente de eventos | La misma serie reproducida evento a evento por Kafka (`replay`) y la API en vivo. Flujo de sensor **simulado de forma realista**; el dato es modelado |
| 10 | Continuidad | Dominio nuevo de este ciclo; continúa el pipeline S6–S11 (infraestructura Kafka/Spark/Grafana) |

---

## 3. Dimensión de análisis

### 3.1 Matriz de asignación

| Integrante | Dimensión | Indicador U1 | Indicador U2 | Fuente batch | Fuente streaming |
|---|---|---|---|---|---|
| Lanzeloth | Aire | MAE de PM2.5 a +1 h (GBT) vs persistencia; PM2.5 mensual/diario | MAE en vivo y mejora % vs persistencia; episodios | `aq_juliaca_2023.json` | topic `atmos-aire` |

### 3.2 Ficha de la dimensión

#### Unidad 1 — Batch

| # | Ítem | Contenido |
|---|---|---|
| 1 | Dimensión y relación con la pregunta | Aire = la variable de interés; mide PM2.5/PM10 y pronostica PM2.5 |
| 2 | Decisión que habilita | Avisar horas con mayor probabilidad de episodio y priorizar reducción de exposición |
| 3 | Indicador U1 | MAE a +1 h: modelo **0,4635 µg/m³** vs persistencia 0,6871 (−32,5 %) |
| 4 | Modelo U1 | GBT (profundidad 3) de MLlib; se entrena una vez al correr el pipeline; split cronológico 80/20 |
| 5 | Fuente batch | JSON horario Open-Meteo/CAMS (`time`, `pm2_5`, `pm10`), API pública |
| 6 | Salida U1 | Notebooks S2–S4 e integrado; Gold Parquet por mes; figuras de MAE, mensual y diario |

#### Unidad 2 — Streaming

| # | Ítem | Contenido |
|---|---|---|
| 7 | Indicador U2 | ¿El modelo sigue ganando a la persistencia en vivo? MAE, horizonte +1 h |
| 8 | Modelo U2 | El mismo GBT con features de 24 h, en Spark Structured Streaming sobre cada dato, comparado con persistencia |
| 9 | Fuente streaming | Eventos `aire.lectura` por Kafka; el histórico de U1 es la acumulación de ese mismo flujo |
| 10 | Salida U2 | Panel Grafana: PM2.5 último, predicción a +1 h, MAE modelo vs persistencia, mejora %, semáforo |

#### Integración y requisitos

| # | Ítem | Contenido |
|---|---|---|
| 11 | Combinación con otras dimensiones | Por definir (Clima se trabaja de forma independiente; correlación aire–temperatura es mejora futura) |
| 12 | Requisitos | Ver lista siguiente |

**Requisitos del sistema**

- Publicar cada lectura horaria como evento `aire.lectura` en Kafka.
- Pronosticar el PM2.5 de la próxima hora y compararlo con la persistencia cuando llegue el dato real.
- Descartar o marcar datos tardíos fuera de 2 h de watermark.
- Publicar `aire.episodio` cuando PM2.5 supere el p95 de entrenamiento.
- Bloquear la ejecución integrada si falla una compuerta de calidad.

### 3.3 Alcance

| Sí cubre | No cubre |
|---|---|
| Pipeline batch + streaming de PM2.5/PM10 de un punto de Juliaca | Red de estaciones |
| Pronóstico a +1 h | Pronóstico a más de 1 h |
| Observabilidad (Prometheus + Grafana) | Otros contaminantes (O₃, NO₂) |
| Integración reproducible con compuertas y manifiesto | Validación contra mediciones reales |
| | Alerta normativa (el ECA nunca se excedió en 2023) |
| | Despliegue en la nube |

---

## 4. Aprobación

| Docente | Fecha | Visto |
|---|---|---|
| Abel Ángel Sullón Macalupú | *pendiente* | *pendiente de visto del docente* |
