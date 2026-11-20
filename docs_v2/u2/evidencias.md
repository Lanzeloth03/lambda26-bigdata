# Evidencias

Capturas tomadas el 20/11/2026 sobre el pipeline en ejecución (Open-Meteo → Kafka → Spark Structured Streaming → Pushgateway → Prometheus → Grafana). Cada bloque indica el criterio de la rúbrica que respalda.

## Integración (corridas con compuertas)
Dos corridas reproducibles con 19 compuertas cada una (previas, calidad, plausibilidad, streaming y observabilidad), con el mismo resultado: 143 h evaluadas, MAE 0,4551 vs 0,6524 (−30,2 %), 19/19 compuertas OK. El MAE del job, el de Pushgateway, el de Prometheus y el recalculado desde Parquet coinciden. Detalle en `u3-integracion/manifiestos/`.
