# Resultados y evidencias – Unidad 1

## Calidad de datos (común)
- Schema explícito
- Nulos = 0
- Duplicados = 0
- PartitionFilters verificado

## Dimensión Clima (Alahin)
- 26 304 registros
- RandomForest: RMSE 1.4351 | R² 0.9180 | MAE 1.1220
- Modelo guardado en `/opt/ecodata/models/clima_rf_model`

## Dimensión Aire (Lanzeloth)
- 2 328 registros por hora (28/05/2026 a 01/09/2026); 2 324 tras la limpieza; 0 duplicados
- Regresión lineal (mejor): RMSE 1,891 | R² 0,891 | MAE 1,321
- Persistencia (base): RMSE 1,944 | R² 0,885 | MAE 1,065
- RandomForest: RMSE 3,676 | GBT: RMSE 3,901 (peor ajuste ante los picos de agosto)
- Riesgo de PM2.5: Bajo 2 308 h, Moderado 4 h, Alto 12 h
- Datos en `Ecodata/artifacts/juliaca_aire_particionado` (Parquet por año y mes)

## Evidencias disponibles
- Notebooks reproducibles
- Parquet particionado
- Tablas comparativas de modelos
- Gráficos de importancia de variables
- Diagrama de arquitectura Lambda