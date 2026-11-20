# EcoData – Sistema de Alerta Temprana de Calidad del Aire (Juliaca)

**Proyecto Sello – Unidad 1 · Big Data**  
**Arquitectura:** Lambda (Batch)  
**Equipo:** EcoData

| Integrante | Dimensión U1 |
|------------|--------------|
| **Alahin Reyme Ticona Veliz** | Predicción de temperatura a partir de variables climáticas |
| **Vargas Marichi Lanzeloth** | Análisis y predicción de calidad del aire (PM2.5 / PM10) orientado a alertas |

---

## Pregunta central del equipo

¿Cómo anticipar condiciones de riesgo ambiental en Juliaca combinando datos climáticos y de calidad del aire mediante un pipeline batch distribuido?

## Resumen de la Unidad 1

Se construyó un **pipeline batch completo** con dos dimensiones complementarias:

1. **Dimensión Clima (Alahin):** limpieza, particionamiento y modelo de regresión de temperatura.
2. **Dimensión Aire (Lanzeloth):** limpieza, análisis y modelo orientado a contaminación / alertas.

Ambas dimensiones comparten la misma arquitectura Lambda y la misma fuente de datos (Open-Meteo).