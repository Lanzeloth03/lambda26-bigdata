# Dimensión U1 – Vargas Marichi Lanzeloth

## Enfoque
Análisis y predicción de **calidad del aire (PM2.5 y PM10)** en Juliaca con el histórico por hora de Open-Meteo (modelo CAMS), orientado a identificar niveles de riesgo y a pronosticar el PM2.5 de la hora siguiente.

## Pregunta de esta dimensión
¿Cómo se comportan los indicadores de contaminación (PM2.5 / PM10) y qué tan bien se puede anticipar el PM2.5 de la próxima hora con un modelo batch?

## Pipeline realizado
1. Extracción de datos de calidad del aire desde Open-Meteo (`juliaca_calidad_aire.csv`)
2. Limpieza y controles de calidad (nulos, duplicados, rangos físicos)
3. Variable de nivel de riesgo (Bajo / Moderado / Alto)
4. Escritura en Parquet particionado por año y mes (`juliaca_aire_particionado`)
5. Entrenamiento y comparación de modelos con Spark MLlib (objetivo: PM2.5 en t+1)
6. Definición de umbrales de alerta

## Datos y calidad
| Elemento | Valor |
|----------|-------|
| Periodo | 28/05/2026 00:00 a 01/09/2026 23:00 (hora Lima) |
| Registros crudos | 2 328 (uno por hora) |
| Nulos en PM2.5 / PM10 | 4 filas con valor faltante, descartadas |
| Duplicados por hora | 0 |
| Registros limpios | 2 324 |
| PM2.5 promedio / mediana / máximo | 7,26 / 5,90 / 64,0 µg/m³ |
| PM2.5 percentil 95 | 16,9 µg/m³ |
| Partición Parquet | `año=2026/mes=5…9` |

**Patrón diario.** El PM2.5 es mayor en la noche y la madrugada (alrededor de 10 µg/m³ entre las 19:00 y las 06:00) y baja a 3–5 µg/m³ entre las 10:00 y las 18:00. Por eso la hora del día es una variable útil.

## Niveles de riesgo
| Nivel | Regla (PM2.5) | Horas | Porcentaje |
|-------|---------------|-------|------------|
| Bajo | < 25 µg/m³ | 2 308 | 99,31 % |
| Moderado | 25 a < 50 µg/m³ | 4 | 0,17 % |
| Alto | ≥ 50 µg/m³ | 12 | 0,52 % |

En este periodo (invierno seco) el aire de Juliaca fue mayormente limpio y los episodios son raros: solo 16 horas superaron 25 µg/m³. Los 12 casos de nivel Alto se concentran en agosto (máximo 64 µg/m³). Esto explica por qué en la Unidad 2 el aviso se define con un umbral **relativo** (percentil 95 de la propia serie) y no solo con el umbral fijo: con un umbral absoluto casi nunca habría alertas que mostrar.

## Modelo (Spark MLlib)
- **Objetivo:** PM2.5 de la hora siguiente.
- **Variables:** PM2.5 actual, rezagos de 1, 2 y 3 horas, media móvil de 6 horas, PM10, hora y mes.
- **Partición temporal:** 80 % para entrenar (1 856 horas) y el 20 % final para evaluar (464 horas), sin mezclar el futuro.
- **Línea base:** persistencia (la próxima hora igual a la actual).

| Modelo | RMSE | MAE | R² |
|--------|------|-----|----|
| Persistencia (base) | 1,944 | 1,065 | 0,885 |
| **Regresión lineal** | **1,891** | 1,321 | **0,891** |
| RandomForest | 3,676 | 1,715 | 0,589 |
| GBT | 3,901 | 1,707 | 0,537 |

**Lectura de resultados.** La regresión lineal logra el menor RMSE y el mayor R², con una mejora pequeña sobre la persistencia; en MAE la persistencia es mejor. Los modelos de árboles rinden peor porque el tramo de prueba incluye los picos de agosto (hasta 64 µg/m³), valores que no vieron al entrenar, y los árboles no extrapolan fuera del rango aprendido. La conclusión honesta es que, con una serie tan estable, es difícil superar a la persistencia por un margen amplio; el valor de la dimensión está en la limpieza, el etiquetado de riesgo y la base para el modelo en tiempo real de U2.

## Umbrales de alerta propuestos
PM2.5 ≥ 25 µg/m³ (Moderado) y ≥ 50 µg/m³ (Alto). En la Unidad 2 se complementan con un umbral relativo al percentil 95 de la serie reciente.

## Aporte al equipo
Esta dimensión entrega la capa de contaminación y los criterios de alerta. Es la base del sistema de alerta temprana que se potencia en la Unidad 2 con streaming (Kafka, Spark Structured Streaming y Grafana).

## Reproducción
Script: `s12/entrena_aire_u1.py` (Spark MLlib). Resultados guardados en `s12/aire_u1_metricas.json`.
