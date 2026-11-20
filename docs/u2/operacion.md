# Operación, escalado y costos

Lo que está implementado se indica como tal; lo que es propuesta de mejora se marca como propuesta.

## 1. Ingesta con Kafka
| Elemento | Decisión implementada |
|---|---|
| Topics | `atmos-aire` (datos), `atmos-negocio` (eventos `aire.episodio`), más los de demostración e histórico |
| Productor | Script Python (`kafka-python`) que lee Open-Meteo y publica un evento JSON por hora |
| Clave del mensaje | `sensorId`, para conservar el orden de una misma fuente |
| Confirmación | `acks="all"`: el productor espera al broker |
| Consumidor | Spark Structured Streaming (`subscribe`), `startingOffsets` configurable, `failOnDataLoss=false` |
| Particiones | 1 partición, factor de replicación 1 (un broker en Docker) |

## 2. Ventanas, watermark y entrega
- Ventana fija de 6 h con watermark de 2 h, modo *append*: la ventana se emite cuando el watermark la cierra; lo que llega más tarde se descarta y se cuenta.
- Checkpoint propio por consulta: tras un reinicio se retoma desde el último lote confirmado.
- Idempotencia por lote en la inferencia: un reintento no duplica métricas ni eventos.
- **Semántica: al menos una vez**, con protección contra duplicados por lote. No se garantiza exactly-once de punta a punta.

## 3. Escalado
Volumen mínimo: 1 evento por hora, ≈ 150 bytes. Una partición y un broker bastan. Ruta de crecimiento:

| Si crece… | Qué se cambia |
|---|---|
| Número de ubicaciones | Más particiones con la ubicación como clave y más ejecutores de Spark |
| Disponibilidad | Kafka con 3 brokers y factor de replicación 3 |
| Estado de Spark | Checkpoint en almacenamiento compartido y `spark.sql.shuffle.partitions` acorde a los núcleos |
| Frecuencia de eventos | Reducir el trigger (hoy 5 s) y vigilar el retraso de consumo |

### Criterios de escalado
| Señal | Umbral | Acción |
|---|---|---|
| El identificador de lote deja de avanzar | > 10 min | Revisar job y fuente |
| Filas de estado crecen sin techo | Sostenido | Revisar el watermark (debe acotar el estado) |
| *Lag* del consumidor | > 10 durante 2 min | Más particiones o ejecutores |
| `mejora_pct` < 0 | 10 min | Reentrenar (deriva) |

## 4. Costos y portabilidad
Componentes de código abierto en contenedores (Kafka, Spark, Prometheus, Pushgateway y Grafana en Docker Compose): se pueden llevar a cualquier proveedor sin servicios propietarios.

!!! info "Precios de referencia"
    AWS us-east-1: t3.medium US$ 0,0416/h; S3 Standard US$ 0,023/GB-mes. Son valores de referencia; verificar antes de usarlos como presupuesto.

| Escenario | Carga | Cómputo | Total aprox. |
|---|---|---|---|
| Laboratorio (hoy) | 1 ubicación | Laptop del equipo, Docker | US$ 0 |
| Referencia de la plantilla del curso | — | 3 × t3.medium | US$ 91,15/mes |
| **Nube mínima (ajustada al volumen)** | 1 ubicación | 1 × t3.medium × 730 h | **≈ US$ 30,4/mes** |
| 100 ubicaciones | 0,03 ev/s | Mismo host (hipótesis a probar) | ≈ US$ 30/mes |
| 1 000 ubicaciones | 0,28 ev/s | 3 brokers + Spark en clúster | No estimado: requiere prueba de carga |

**Justificación.** Con 24 eventos al día, un clúster de tres nodos o un servicio gestionado de Kafka y Spark estaría sobredimensionado. El costo lo domina el cómputo siempre encendido, no los datos. Son extrapolaciones lineales, no pruebas de carga.
