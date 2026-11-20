# Resultados y evidencias – Unidad 1

## Calidad de datos (común)
- Schema explícito
- Nulos = 0
- Duplicados = 0
- PartitionFilters verificado

## Dimensión Aire (Lanzeloth), año 2023 completo
- 8 760 registros por hora, 0 nulos, 0 duplicados, 0 negativos, 0 huecos horarios
- Gold en Parquet particionado por `mes` (12 particiones); el plan de ejecución muestra `PartitionFilters` al consultar un mes
- Partición cronológica: 6 988 h de entrenamiento y 1 747 h de prueba

| Modelo | RMSE | R² | MAE |
|---|---|---|---|
| Persistencia (base) | 1,5067 | 0,8478 | 0,6871 |
| Regresión lineal | 1,4185 | 0,8651 | 0,7419 |
| Ridge | 1,4374 | 0,8615 | 0,7459 |
| Elastic Net | 1,4353 | 0,8619 | 0,7094 |
| Random Forest | 1,0727 | 0,9229 | 0,4780 |
| GBT profundidad 4 | 1,0656 | 0,9239 | 0,4734 |
| **GBT profundidad 3 (ganador)** | **1,0498** | **0,9261** | **0,4635** |

El GBT reduce el MAE **32,5 %** frente a la persistencia; el modelo guardado se recargó y reproduce 0,4635.

### Primera corrida (4 meses de 2026)
2 328 horas (28/05 a 01/09/2026): ganó la regresión lineal (RMSE 1,891) y los árboles rindieron peor por los picos de agosto. Con un año completo hay suficientes ciclos diarios y el GBT supera a todos. Ver [Dimensión Aire (Lanzeloth)](dimension-lanzeloth.md).

## Evidencias disponibles
- Notebooks reproducibles
- Parquet particionado
- Tablas comparativas de modelos
- Gráficos de importancia de variables
- Diagrama de arquitectura Lambda