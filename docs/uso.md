# 3. Uso del sistema

## Flujo principal (lo que se usa en la demo)
| Paso | Dónde | Qué ves |
|---|---|---|
| 1. Levantar | `scripts\levantar.cmd` | 5 servicios OK |
| 2. Ejecutar la integración | Jupyter → `13_integracion_dataops.ipynb` → *Restart & Run All* | Compuertas `[OK]`, MAE de producción, confirmación en Prometheus, manifiesto |
| 3. Repetir | *Restart & Run All* otra vez | La tabla de la sección 7 compara las 2 corridas: deben ser idénticas |
| 4. Ver el tablero | Grafana → *U3 - Integración Aire* | MAE modelo vs persistencia, mejora %, compuertas, duración |
| 5. Ver el streaming en vivo | Grafana → *S12 - Calidad del aire* (job `lambda26_aire`) | PM2.5, predicción, MAE, watermark |
| 6. Ver el tráfico | Kafka UI <http://localhost:48085> | topics `atmos-aire`, `atmos-negocio`, y uno `atmos-aire-int-<runId>` por corrida |

## Streaming en vivo (U2)
```bat
:: pestaña 1: job de aire (no cerrar, no duplicar)
docker exec -it lambda26-pyspark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /tmp/s12/scripts/s12_streaming_aire.py --modelo /tmp/s12/modelo_pm25 --limpiar
:: pestaña 2: publicar datos
docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_a_kafka.py --archivo /tmp/s12/aq_juliaca_2023.json --desde 2023-12-01T00:00 --pausa 0.05
docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_a_kafka.py --api            :: últimas horas reales (fuera de muestra)
docker exec lambda26-pyspark python /tmp/s12/scripts/openmeteo_a_kafka.py --api --tardio 30 :: dato tardío: prueba del watermark
```

## Batch (U1)
Notebooks en `u1-aire/`: `S2_aire_pyspark`, `S3_aire_calidad_parquet`, `S4_aire_modelo_mllib` y el integrado `u1_aire_lanzeloth.ipynb`. Cópialos al contenedor y ejecútalos con *Restart & Run All*. La evidencia ya ejecutada (Gold Parquet, modelo, figuras) está en `u1-aire_corrida/`.

## Casos de uso
1. **Operador:** abre Grafana y ve el último PM2.5, el pronóstico a +1 h y si el modelo sigue ganándole a la persistencia (alerta `AireModeloNoSuperaPersistencia`).
2. **Analista:** consulta el Gold mensual en Parquet (`PartitionFilters mes=…`) para tendencias.
3. **Revisor/docente:** reproduce la corrida de integración y compara manifiestos.
