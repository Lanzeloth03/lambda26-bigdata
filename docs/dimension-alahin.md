# Dimensión U1 – Alahin Reyme Ticona Veliz

## Enfoque
Predicción de **temperatura** en Juliaca a partir de variables climáticas (viento, humedad, hora y mes), como base para anticipar condiciones ambientales.

## Pregunta de esta dimensión
¿Qué tan bien se puede predecir la temperatura en Juliaca usando velocidad del viento, dirección del viento, humedad relativa, hora del día y mes?

## Pipeline realizado
1. Extracción de datos climáticos de Open-Meteo (2023-2025)
2. Limpieza y controles de calidad (schema, 0 nulos, 0 duplicados)
3. Escritura en Parquet particionado por año/mes (Capa Gold)
4. Entrenamiento y comparación de 4 modelos de regresión
5. Guardado del modelo ganador (RandomForest)

## Resultados obtenidos

| Métrica | Valor |
|---------|-------|
| Registros procesados | 26 304 |
| Mejor modelo | **RandomForestRegressor** |
| RMSE | **1.4351** |
| R² | **0.9180** |
| MAE | **1.1220** |

### Importancia de variables
| Variable | Importancia |
|----------|-------------|
| hour | 0.6360 |
| relative_humidity_2m | 0.2005 |
| wind_direction_10m | 0.0803 |
| mes | 0.0709 |
| wind_speed_10m | 0.0124 |

La **hora del día** es el predictor más fuerte de la temperatura en Juliaca.

## Aporte al equipo
Esta dimensión entrega la base climática confiable y el primer modelo predictivo del sistema. En la Unidad 2 se combinará con la dimensión de calidad del aire para generar alertas tempranas.