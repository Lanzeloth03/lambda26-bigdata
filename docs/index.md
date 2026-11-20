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
