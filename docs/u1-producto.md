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
- Datos: año **2023** completo, 8 760 horas, sin huecos ni duplicados; Gold en Parquet particionado por `mes` (12 particiones)
- Objetivo: PM2.5 a +1 h
- Partición cronológica: 6 988 h de entrenamiento y 1 747 h de prueba (desde 2023-10-20)
- Mejor modelo: **GBT de profundidad 3** (Spark MLlib): MAE **0,4635** frente a 0,6871 de la persistencia (**−32,5 %**), RMSE 1,0498, R² 0,9261
- Otros modelos (MAE): regresión lineal 0,7419 · Ridge 0,7459 · Elastic Net 0,7094 · Random Forest 0,4780 · GBT profundidad 4 0,4734 · GBT profundidad 5 0,4813
- Primera corrida con 4 meses de 2026 (2 328 h): ganó la regresión lineal; se conserva en [Dimensión Aire (Lanzeloth)](dimension-lanzeloth.md)

## 6. Conclusión
Se completó el pipeline batch de la Unidad 1 para la dimensión Aire, con un año completo de histórico. La base está lista para incorporar streaming e inferencia en tiempo real en la Unidad 2.
