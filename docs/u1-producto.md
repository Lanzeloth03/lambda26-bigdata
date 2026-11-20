# Producto Unidad 1 – EcoData (Juliaca)

**Autor:** Lanzeloth Vargas Marichi
**Arquitectura:** Lambda (Batch)

---

## 1. Pregunta central
¿Cómo anticipar la calidad del aire en Juliaca (PM2.5 y PM10) mediante un pipeline batch distribuido?

## 2. Dimensión U1

| Integrante | Dimensión | Variable objetivo |
|------------|-----------|-------------------|
| Lanzeloth Vargas Marichi | Calidad del aire y alertas | pm2_5 / pm10 / nivel_riesgo |

## 3. Arquitectura
**Lambda**. La Unidad 1 implementa la ruta Batch. La ruta Speed se desarrolla en Unidad 2.

## 4. Pipeline
Open-Meteo → Limpieza + Calidad → Parquet particionado (Gold) → Spark MLlib → Modelo guardado

## 5. Resultados principales

### Dimensión Aire (Lanzeloth)
- Registros: 2 328 por hora (2 324 limpios), del 28/05/2026 al 01/09/2026
- Objetivo: PM2.5 a +1 h
- Mejor modelo: regresión lineal (Spark MLlib)
- Métricas en prueba: RMSE 1,891 | R² 0,891 | MAE 1,321 (persistencia: RMSE 1,944 | MAE 1,065)
- Riesgo: 99,3 % Bajo, 4 horas Moderado, 12 horas Alto

## 6. Conclusión
Se completó el pipeline batch de la Unidad 1 para la dimensión Aire. La base está lista para incorporar streaming e inferencia en tiempo real en la Unidad 2.
