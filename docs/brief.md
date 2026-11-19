# Brief técnico-analítico

*Plantilla del curso, completada para la dimensión Aire.* Se actualiza solo si el alcance cambia de verdad.

## 1. Datos del equipo
| Campo | Valor |
|---|---|
| Nombre del equipo | EcoData (dimensión Aire: Lanzeloth Vargas Marichi) |
| Sección | G1 — Big Data, semestre 2026-2, campus Juliaca |
| URL del repositorio | <https://github.com/Lanzeloth03/lambda26-bigdata> |
| Topics configurados | Lista en [Repositorio y estándares](repositorio.md). **Estado: configurar en GitHub** (incluye `grupo-<n>-ecodata-aire`) |

| Integrante | Rol / énfasis |
|---|---|
| Lanzeloth Vargas Marichi | Batch/Spark, streaming/Kafka, observabilidad y ML de la dimensión Aire (extremo a extremo) |

## 2. Dominio del proyecto
1. **Nombre:** EcoData Aire — alerta temprana de calidad del aire en Juliaca.
2. **Problema:** Juliaca no cuenta con monitoreo accesible de material particulado; se necesita anticipar el PM2.5 de la próxima hora y avisar de episodios.
3. **Dominio de datos:** ambiental / salud pública; se mide PM2.5 y PM10 (µg/m³) cada hora.
4. **Variable de interés:** concentración de PM2.5 (calidad del aire).
5. **Pregunta central:** ¿Un modelo entrenado con las últimas horas de PM2.5 y PM10 predice el PM2.5 de la próxima hora mejor que repetir la última hora?
6. **Usuarios:** operador/autoridad ambiental local, analista, docente-revisor.
7. **Arquitectura:** **Lambda** — histórico anual por lotes + flujo continuo por eventos con el mismo modelo (ver [Arquitectura](arquitectura.md)).
8. **Fuente batch:** Open-Meteo Air Quality (CAMS), año 2023, 8 760 h, Juliaca (−15,5; −70,13).
9. **Fuente de eventos:** la misma serie reproducida evento a evento por Kafka (`replay`) y la API en vivo (últimas horas): flujo de sensor **simulado de forma realista**; el dato es modelado.
10. **Continuidad:** dominio nuevo de este ciclo; continúa el pipeline S6–S11 del equipo (infraestructura Kafka/Spark/Grafana).

## 3. Dimensión de análisis

### Matriz de asignación
| Integrante | Dimensión | Indicador U1 | Indicador U2 | Fuente batch | Fuente streaming |
|---|---|---|---|---|---|
| Lanzeloth | Aire | MAE de PM2.5 a +1 h (GBT) vs persistencia; PM2.5 mensual/diario | MAE en vivo y mejora % vs persistencia; episodios | `aq_juliaca_2023.json` | topic `atmos-aire` |

### Ficha de la dimensión (12 ítems)
1. **Dimensión y relación con la pregunta:** Aire = la variable de interés; mide PM2.5/PM10 y pronostica PM2.5.
2. **Decisión que habilita:** avisar horas con mayor probabilidad de episodio y priorizar reducción de exposición.
3. **Indicador U1:** ¿cuánto error tiene el pronóstico a +1 h? KPI = MAE; valor del modelo: **0,4635 µg/m³** (persistencia 0,6871; −32,5 %).
4. **Modelo U1:** GBT (profundidad 3) de MLlib; se entrena **una vez** al correr el pipeline; split cronológico.
5. **Fuente batch:** JSON horario de Open-Meteo/CAMS (`time`, `pm2_5`, `pm10`), origen API pública.
6. **Salida U1:** notebooks S2–S4 e integrado; Gold Parquet por mes; figuras de MAE, mensual y diario.
7. **Indicador U2:** ¿el modelo sigue ganando a la persistencia en vivo? Métrica = MAE; horizonte **+1 h**.
8. **Modelo U2:** el mismo GBT (series de tiempo con features de 24 h) corriendo en Spark Structured Streaming sobre cada dato, comparado con persistencia.
9. **Fuente streaming:** eventos `aire.lectura` por Kafka; **el histórico de U1 es la acumulación de ese mismo flujo** (misma serie).
10. **Salida U2:** panel en Grafana: PM2.5 último, predicción a +1 h, MAE modelo vs persistencia, mejora %, semáforo de actividad.
11. **Combinación con otras dimensiones:** por definir (el compañero trabaja Clima de forma independiente; correlación aire–temperatura es una mejora futura).
12. **Requisitos:**
    * El sistema debe publicar cada lectura horaria como evento `aire.lectura` en Kafka.
    * El sistema debe pronosticar el PM2.5 de la próxima hora y compararlo con la persistencia cuando llegue el dato real.
    * El sistema debe descartar o marcar datos tardíos fuera de 2 h de watermark.
    * El sistema debe publicar `aire.episodio` cuando PM2.5 supere el p95 de entrenamiento.
    * El sistema debe bloquear la ejecución integrada si falla una compuerta de calidad.

### Alcance
**Sí cubre:** pipeline batch + streaming de PM2.5/PM10 de un punto de Juliaca; pronóstico a +1 h; observabilidad; integración reproducible con compuertas y manifiesto.
**No cubre:** red de estaciones; pronóstico a más de 1 h; otros contaminantes (O₃, NO₂); validación contra mediciones reales; alerta normativa (ECA nunca se excedió en 2023); despliegue en la nube.

## 4. Aprobación
| Docente | Fecha |
|---|---|
| Abel Ángel Sullón Macalupú | *pendiente de visto del docente* |
