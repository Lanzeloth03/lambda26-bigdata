# Contrato de eventos

El contrato vive en un solo lugar del código (`s12-aire/scripts/aire_comun.py`), de modo que productor, Spark y pruebas usan el mismo esquema.

## Evento de sensor: `aire.lectura`
```json
{"tipoEvento":"aire.lectura","sensorId":"openmeteo-cams-juliaca",
 "pm25":6.5,"pm10":9.4,"origen":"openmeteo-replay","timestamp":1701388800000}
```
| Campo | Tipo | Significado |
|---|---|---|
| `tipoEvento` | texto | Siempre `aire.lectura` |
| `sensorId` | texto | Identifica la fuente; es la **clave** del mensaje en Kafka |
| `pm25` | decimal | PM2.5 en µg/m³ (≥ 0) |
| `pm10` | decimal | PM10 en µg/m³ (≥ 0) |
| `origen` | texto | `openmeteo-replay` (reproducción), `openmeteo-cams` (API en vivo) o `demo-tardio` (prueba del watermark) |
| `timestamp` | entero | Hora del dato en milisegundos (**tiempo de evento**, no de llegada) |

Tamaño ≈ 150 bytes; periódico (1 por hora) y de tamaño constante.

## Evento de negocio: `aire.episodio`
Se publica en `atmos-negocio` cuando PM2.5 supera el p95 del entrenamiento (10,5 µg/m³). Es irregular y lo origina una regla, no la fuente.

## Topics
| Topic | Contenido |
|---|---|
| `atmos-aire` | Telemetría `aire.lectura` |
| `atmos-negocio` | Eventos `aire.episodio` |
| `atmos-aire-demo` | Reproducción de la demostración |
| `atmos-aire-hist-2024`, `atmos-aire-hist-2025` | Histórico real adicional cargado a Kafka |
| `atmos-aire-int-*` | Una corrida de integración por topic |

Todos con 1 partición y factor de replicación 1 (un broker en Docker).

## Reglas
- **Clave** = `sensorId`: los eventos de una misma fuente conservan el orden.
- **Validación**: esquema y rango físico (PM ≥ 0); lo que no cumple se rechaza o se marca.
- **Entrega**: `acks="all"` en el productor y checkpoint en Spark → al menos una vez, con resultados idempotentes por lote.
- **Tiempo**: el procesamiento usa `timestamp` (tiempo de evento) para ventanas y watermark.
