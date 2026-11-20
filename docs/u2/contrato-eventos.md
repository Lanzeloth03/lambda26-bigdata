# Contrato de eventos (S6 y S7)

Los eventos se publican como JSON con **esquema versionado** (carpeta `schemas/`). Un cambio incompatible sería `v2` en un topic nuevo.

| Topic | Tipo | Esquema | Clave | Frecuencia |
|---|---|---|---|---|
| `atmos-aire` | Telemetría de sensor (modelada) | `schemas/atmos-aire-v1.schema.json` | `sensorId` | 1 por hora |
| `atmos-negocio` | **Evento empresarial derivado** | ver abajo | `sensorId` | solo cuando ocurre |

## Eventos de negocio (`atmos-negocio`)
Se generan en Spark cuando se cumple una regla del negocio, no vienen de la fuente:

| `tipoEvento` | Regla | Para qué sirve |
|---|---|---|
| `aire.episodio` | PM2.5 de la hora > p95 del entrenamiento (10,5 µg/m³) | Alerta temprana de calidad del aire |

## Diferencia entre un evento de sensor y uno de negocio
- **Sensor (`aire.lectura`):** periódico, tamaño fijo, se valida por rango físico (PM ≥ 0).
- **Negocio (`aire.episodio`):** irregular, nace de una decisión, y es lo que consume quien actúa (alerta).

## Garantías
- **Al menos una vez**, con resultados idempotentes por hora: el estado se calcula por la hora del dato (`timestamp`), no por el momento de llegada.
- **Orden:** la clave `sensorId` envía todos los eventos de una ubicación a la misma partición.
- **Datos tardíos:** más de 2 h de retraso respecto al watermark se descartan y se cuentan (`filas_descartadas_watermark_total`).
- No se afirma «exactamente una vez».

## Verificación en vivo
1. Kafka UI → `atmos-aire` → Messages: se ve la clave y el JSON.
2. Notebook, sección 4: conteo de eventos y últimos mensajes.
