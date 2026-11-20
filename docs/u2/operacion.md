# Operación: ingesta, alertas, observabilidad, escalado y costos

Esta página reúne las decisiones operativas del pipeline de la Unidad 2 (dimensión Aire). Lo que ya está implementado se indica como tal; lo que es una propuesta de mejora se marca como **propuesta**.

## 1. Ingesta con Kafka

| Elemento | Decisión implementada |
|---|---|
| Topics | `atmos-aire` (datos) y `atmos-negocio` (eventos `aire.episodio`) |
| Productor | Script Python (`kafka-python`) que lee Open-Meteo y publica un evento JSON por hora |
| Clave del mensaje | `sensorId`, para mantener el orden de una misma fuente |
| Confirmación | `acks="all"`: el productor espera la confirmación del broker antes de dar el envío por bueno |
| Contrato | JSON con marca de tiempo del evento, valores medidos y campo `origen` (p. ej. `demo-tardio` para probar el watermark) |
| Consumidor | Spark Structured Streaming (`subscribe`), `startingOffsets` configurable y `failOnDataLoss=false` |
| Particiones | 1 partición, factor de replicación 1 (un solo broker en Docker) |

**Patrón de ingesta:** productor único por fuente → topic → un consumidor Spark con dos consultas (ventanas y inferencia) que comparten el mismo flujo.

## 2. Alertas y umbrales

### Alertas de negocio (implementadas)
| Evento | Regla | Umbral |
|---|---|---|
| `aire.episodio` | PM2.5 por encima del percentil 95 del histórico de entrenamiento | p95 relativo (se guarda con el modelo) |

### Umbrales operativos: latencia, throughput y errores
| Dimensión operativa | Métrica en Grafana | Umbral |
|---|---|---|
| Latencia / frescura | «Segundos desde el último push» (`time() - push_time_seconds`) | Verde < 120 s · naranja ≥ 120 s · rojo ≥ 600 s (implementado en el tablero) |
| Throughput | «Eventos procesados» (`lambda26_*_eventos_total`) | Esperado: 1 evento por hora. Si el contador no crece durante más de 2 horas, la fuente o el productor están caídos (**propuesta**) |
| Errores | «Filas descartadas por watermark» y estado de la consulta | Cualquier valor mayor que 0 se revisa; el valor sube a propósito con el evento `demo-tardio` |

**Implementado y verificado (04/11/2026):** cinco reglas de Prometheus en `s12/operacion/prometheus/alerts.yml`, cargadas con `rule_files` y visibles en `http://localhost:49090/alerts`. La de frescura (`AireSinEventosNuevos`) usa `absent()` para disparar también cuando el job nunca empujó métricas. Se comprobó el ciclo completo: al dejar de llegar datos la frescura superó 600 s, el semáforo pasó a rojo y la alerta pasó a *Firing*; al publicar de nuevo se resolvieron. Detalle y lista completa en [Observabilidad y alertas](observabilidad-alertas.md).

## 3. Observabilidad

- **Métricas:** cada consulta de Spark publica sus indicadores en Pushgateway con el prefijo `lambda26_` (eventos, MAE del modelo y de la persistencia, episodios, filas descartadas, estado y marca del watermark). Prometheus los recoge y Grafana los muestra en un solo tablero con la sección de aire.
- **Progreso de la consulta:** un `StreamingQueryListener` lee el avance de cada lote (`numRowsDroppedByWatermark`, filas en estado, watermark) y lo envía como métrica.
- **Logs:** Spark en nivel `WARN` y mensajes de cada script por consola. Los logs en formato JSON con campos fijos (nivel, componente, id de lote) son una **mejora pendiente**; por ahora la información estructurada viaja en las métricas.
- **Kafka:** inspección de topics y mensajes con Kafka UI.

## 4. Ventanas, watermark y semántica de entrega

- **Ventana fija de 6 h** con **watermark de 2 h**, salida en modo `append`: la ventana se emite cuando el watermark la cierra, y los eventos que llegan más tarde se descartan y se cuentan en Grafana.
- **Checkpoint** propio por consulta (`_ckpt/ventanas` y `_ckpt/inferencia`): si el job se reinicia, retoma desde el último lote confirmado.
- **Idempotencia por lote:** la consulta de inferencia ignora un lote cuyo identificador ya procesó, de modo que un reintento no duplica métricas ni eventos.
- **Semántica:** el productor usa `acks="all"` y Spark usa checkpoint, lo que da entrega **al menos una vez** con protección contra duplicados por lote. **No se garantiza exactly-once de punta a punta**, y así consta en el alcance del brief.

## 5. Escalado

El volumen de datos nuevos es mínimo (un evento por hora, menos de 1 KB cada uno). Medido en Kafka UI el 04/11/2026, los topics acumulan `atmos-aire` 12 845 mensajes (2 MB) y `atmos-negocio` 1 125 (220 KB). Tras cargar los años 2024 y 2025 el broker acumula alrededor de 27 000 mensajes, sin cambios de configuración: un solo broker sigue siendo suficiente. Por tanto una sola partición y un solo broker son suficientes. Ruta de crecimiento:

| Si crece… | Qué se cambia |
|---|---|
| Número de estaciones o ubicaciones | Más particiones por topic usando la estación como clave, y un consumidor Spark con más executors |
| Disponibilidad | Kafka con 3 brokers y factor de replicación 3 |
| Estado de Spark | Checkpoint en almacenamiento compartido (objeto o HDFS) y `spark.sql.shuffle.partitions` acorde al número de núcleos |
| Frecuencia de eventos | Reducir el `trigger` (hoy 5 s) y vigilar el retraso de consumo |

## 6. Costos y portabilidad (cloud-agnostic)

**Decisión: componentes de código abierto en contenedores.** Kafka, Spark, Prometheus, Pushgateway y Grafana corren en Docker Compose y se pueden llevar sin cambios a cualquier proveedor (máquinas virtuales, Kubernetes o servicios gestionados con API compatible con Kafka). No se usa ningún servicio propietario de un proveedor.

| Escenario | Costo mensual aproximado | Comentario |
|---|---|---|
| Entorno actual (laptop, Docker) | US$ 0 | Open-Meteo es gratuita para uso no comercial |
| Referencia de la plantilla del curso: 3 × t3.medium | US$ 91,15 | Margen para separar Kafka, Spark y observabilidad |
| Alternativa ajustada al volumen: 1 × t3.medium | ≈ US$ 30,4 | Un tercio de la referencia (91,15 ÷ 3); alcanza para el volumen actual |

**Justificación:** con 24 eventos al día, un clúster de tres nodos o un servicio gestionado de Kafka y Spark estaría sobredimensionado. Se recomienda empezar con una sola máquina y escalar por partes, según la tabla anterior, cuando aumente el número de estaciones. Las cifras son de referencia (precios bajo demanda); deben verificarse en la calculadora del proveedor antes de contratar.
