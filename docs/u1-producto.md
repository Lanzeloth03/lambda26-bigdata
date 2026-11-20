# Producto Unidad 1 – EcoData (Juliaca)

**Equipo:** EcoData  
**Arquitectura:** Lambda (Batch)  
**Integrantes:**  
- Alahin Reyme Ticona Veliz  
- Vargas Marichi Lanzeloth

---

## 1. Pregunta central del equipo
¿Cómo anticipar condiciones de riesgo ambiental en Juliaca combinando datos climáticos y de calidad del aire mediante un pipeline batch distribuido?

## 2. Dimensiones U1

| Integrante | Dimensión | Variable objetivo |
|------------|-----------|-------------------|
| Alahin Reyme Ticona Veliz | Predicción climática | temperature_2m |
| Vargas Marichi Lanzeloth | Calidad del aire y alertas | pm2_5 / pm10 / nivel_riesgo |

## 3. Arquitectura
**Lambda**. La Unidad 1 implementa la ruta Batch. La ruta Speed se desarrolla en Unidad 2.

## 4. Pipeline común
Open-Meteo → Limpieza + Calidad → Parquet particionado (Gold) → Spark MLlib → Modelo guardado

## 5. Resultados principales

### Dimensión Clima (Alahin)
- Registros: 26 304
- Mejor modelo: RandomForest
- RMSE: 1.4351 | R²: 0.9180 | MAE: 1.1220
- Variable más importante: hour (0.636)

### Dimensión Aire (Lanzeloth)
- Registros: 2 328 por hora (2 324 limpios), del 28/05/2026 al 01/09/2026
- Objetivo: PM2.5 a +1 h
- Mejor modelo: regresión lineal (Spark MLlib)
- Métricas en prueba: RMSE 1,891 | R² 0,891 | MAE 1,321 (persistencia: RMSE 1,944 | MAE 1,065)
- Riesgo: 99,3 % Bajo, 4 horas Moderado, 12 horas Alto

## 6. Conclusión
Se completó el pipeline batch de la Unidad 1 con dos dimensiones complementarias. La base está lista para incorporar streaming e inferencia en tiempo real en la Unidad 2.