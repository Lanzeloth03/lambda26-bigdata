# EcoData – Sistema de Alerta Temprana de Calidad del Aire (Juliaca)

**Proyecto Sello – Unidades 1 y 2 · Big Data**
**Arquitectura:** Lambda (batch + streaming)
**Autor:** Lanzeloth Vargas Marichi

| Integrante | Dimensión |
|------------|-----------|
| **Lanzeloth Vargas Marichi** | Análisis y predicción de calidad del aire (PM2.5 / PM10) orientado a alertas |

---

## Pregunta central

¿Cómo anticipar el PM2.5 de la próxima hora en Juliaca y avisar cuando se entra en un episodio de contaminación, mediante un pipeline batch distribuido y un flujo en tiempo real?

## Resumen

Se construyó un **pipeline batch completo** (Unidad 1) y un **sistema en tiempo real** (Unidad 2) para la dimensión Aire:

1. **Unidad 1 (batch):** limpieza, particionamiento en Parquet y modelo de regresión de PM2.5 con Spark MLlib.
2. **Unidad 2 (streaming):** eventos por Kafka, Spark Structured Streaming con ventanas y watermark, modelo que pronostica la hora siguiente, métricas en Prometheus y tablero en Grafana.

Ambas unidades comparten la arquitectura Lambda y la misma fuente de datos (Open-Meteo, modelo CAMS).

## Resultados medidos
| Escenario | MAE modelo | MAE persistencia | Mejora |
|---|---|---|---|
| U1 · batch, prueba cronológica (1 747 h, año 2023) | **0,4635** µg/m³ | 0,6871 | **−32,5 %** |
| U2 · streaming en vivo (761 h) | **0,422** µg/m³ | 0,567 | **−25,5 %** |
| Integración · 19 compuertas (143 h) | **0,4551** µg/m³ | 0,6524 | **−30,2 %** |
| Validación fuera de muestra · 2024 (8 759 h) | **0,4762** µg/m³ | 0,7095 | **−32,9 %** |
| Validación fuera de muestra · 2025 (8 735 h) | **0,5130** µg/m³ | 0,6522 | **−21,3 %** |
| Validación fuera de muestra · 2026 (7 751 h, hasta el 20/11) | **0,6335** µg/m³ | 0,8349 | **−24,1 %** |

## Datos
Año **2023** completo (8 760 h) para el pipeline batch y el entrenamiento; años **2024 y 2025** cargados a Kafka como histórico real y usados, junto con 2026, para validar el modelo; lecturas recientes por la API de Open-Meteo cada hora. Más de **27 000 mensajes** en los topics del broker. Ver [Datos y topics](u2/datos-y-topics.md).
