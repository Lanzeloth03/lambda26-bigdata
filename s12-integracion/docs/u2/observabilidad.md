# Observabilidad (S9 y S12)

## Qué se mide y de dónde sale

| Capa | Fuente | Métricas |
|---|---|---|
| Broker Kafka | `kafka-exporter` (scrape cada 15 s) | mensajes/s por topic, lag del consumidor, brokers vivos |
| Spark (por evento) | campos `isValid`, `processedAt`, `latencyMs = processedAt − timestamp` | latencia promedio y p95, eventos inválidos |
| Spark (por micro-lote) | `lastProgress` / `recentProgress` | filas/s de entrada y proceso, duración del lote, atraso de offsets, watermark, filas de estado, agregados tardíos descartados |
| Modelo | el propio job | valor real, predicción, MAE del modelo y de la persistencia |
| Negocio | consulta `negocio` | eventos por tipo y severidad |

El job publica al **Pushgateway** con `PUT` (reemplaza el grupo `job/instance`; cada consulta usa su instancia) y Prometheus lo visita con `honor_labels: true`. Los nombres llevan el prefijo `lambda26_s12_`.

## Tablero «S12 - Pipeline U2 en vivo»

Cuatro filas, 28 paneles, refresco de 5 s:

1. **Estado del pipeline:** segundos desde el último lote (job vivo), eventos IoT procesados, eventos de negocio, atraso de Spark, consultas activas, inválidos.
2. **Predicción en vivo:** real vs predicción alineada, MAE modelo vs persistencia, cociente modelo/persistencia, error del último par.
3. **Rendimiento y correlación:** mensajes/s del broker frente a filas/s de entrada y proceso de Spark; latencia promedio y p95; duración de lote y atraso por consulta.
4. **Ventanas, watermark y checkpoint:** retraso del watermark, ventanas emitidas, agregados tardíos descartados, filas de estado, última ventana (n, promedio, mín., máx.).

**Correlación:** el panel 3 pone en la misma gráfica lo que entra al topic (kafka-exporter), lo que Spark lee y lo que procesa; si el broker recibe más de lo que Spark procesa, el atraso crece y la latencia sube.

## Alertas propuestas (umbrales medidos en S9)

| Situación | Regla | Dato que la justifica |
|---|---|---|
| Kafka no visible | `kafka_brokers < 1` | normal: 1 broker |
| Exporter caído | `up{job="kafka-exporter"} == 0` | normal: 1 |
| Lag alto sostenido | `kafka_consumergroup_lag > 10` durante 2 min | lag normal 0–1 |
| Latencia alta (sensible) | latencia promedio > 4 000 ms por minuto, 3 min seguidos | promedio medido 3 596 ms |
| Latencia alta (crítica) | p95 > 12 000 ms en un minuto | p95 normal por minuto 5 003–6 004 ms |
| Job detenido | `time() - push_time_seconds{job="lambda26_s12",instance="inferencia"} > 90` | un lote cada 5 s |
| Deriva del modelo | MAE modelo / MAE persistencia > 2 sostenido | S11 midió 16× tras un cambio de régimen |

## Lectura honesta de la latencia

La latencia de ~3,6 s no es lentitud de Spark: es el trigger de 5 s frente al productor de 3 s y la resolución de 1 s del `timestamp` del ESP32. Una ventana con p95 de 13 s coincidió con una irregularidad en los mensajes por segundo, lo que apunta a una demora aguas arriba (WiFi/MQTT).
