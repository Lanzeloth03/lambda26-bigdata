# Ingesta de eventos (S6 y S7)

Un mismo Kafka recibe dos flujos: lecturas del sensor (IoT) y eventos de negocio derivados del monitoreo.

## Eventos IoT (S7)

| Elemento | Valor |
|---|---|
| Dispositivo | ESP32 con sensor ultrasónico HC-SR04 (se sustituyó el DHT11, que nunca devolvió lecturas) |
| Frecuencia | 1 lectura cada 3 s (≈ 0,33 eventos/s, ≈ 1 177 eventos/h) |
| Transporte | WiFi → MQTT público `test.mosquitto.org`, topic `lambda26/atmos-real/lanzeloth/lecturas` |
| Puente | `bridge.py` reenvía cada mensaje a Kafka |
| Topic Kafka | `atmos-real` · 3 particiones · replicación 1 · clave = `sensorId` |
| Volumen medido | ≈ 2 449 eventos acumulados al 22/10/2026 (S10); 2,65 MB/h como cota alta de Parquet (S9) |

**Contrato del evento** (valores de ejemplo):

```json
{
  "tipoEvento": "sensor.lectura",
  "sensorId": "esp32-fisico-lanzeloth",
  "distanciaCm": 118.6,
  "origen": "esp32-fisico",
  "timestamp": 1761152400000
}
```

| Campo | Tipo | Regla de validación |
|---|---|---|
| `tipoEvento` | string | no nulo |
| `sensorId` | string | no nulo; es la clave de partición |
| `distanciaCm` | double | entre 2 y 400 cm (rango físico del HC-SR04); fuera de rango se marca `isValid = false` |
| `origen` | string | informativo |
| `timestamp` | long | epoch en **ms** del ESP32 (NTP, resolución de 1 s); es el *tiempo del evento* |

Como todos los eventos usan la misma clave, **una sola partición está activa**; las otras dos quedan vacías (lag −1 en el exporter). Para varios sensores, claves distintas reparten la carga.

## Eventos de negocio (S6)

Servicio de alertas del monitoreo (`scripts/s6_eventos_negocio.py`). Cada decisión operativa se publica como un evento de negocio, independiente de la lectura cruda.

| Elemento | Valor |
|---|---|
| Topic | `atmos-negocio` · 3 particiones · replicación 1 · clave = `sensorId` |
| Productor 1 | `servicio`: consume `atmos-real` y publica `AlertaOperativa` cuando la lectura sale de 2–400 cm (severidad alta) o cambia ≥ 20 cm de una lectura a la siguiente (media; alta si ≥ 50 cm). Enfriamiento de 30 s por regla |
| Productor 2 | `orden`: un operador emite una `OrdenMantenimiento` (evento simulado, uno por comando) |
| Consumidores | `consumir` (consola, en vivo) y la consulta `negocio` del job Spark, que cuenta eventos por tipo y severidad |
| Entrega | productor con `acks=all`; el `eventId` (UUID) permite deduplicar aguas abajo (hoy no se deduplica) |

```json
{
  "eventId": "6f1c0a9e-3a52-4c1e-9d0b-2f1d7b8c9a10",
  "tipoEvento": "AlertaOperativa",
  "version": 1,
  "sensorId": "esp32-fisico-lanzeloth",
  "severidad": "media",
  "detalle": "Cambio brusco de nivel: 118.6 -> 18.2 cm (-100.4 cm en una lectura)",
  "valorAnteriorCm": 118.6,
  "valorActualCm": 18.2,
  "timestamp": 1761152403000,
  "origen": "alertas-service"
}
```

| Campo | Tipo | Notas |
|---|---|---|
| `eventId` | string (UUID) | identificador único; permite deduplicar |
| `tipoEvento` | string | `AlertaOperativa` \| `OrdenMantenimiento` |
| `version` | int | versión del contrato (1) |
| `severidad` | string | `baja` \| `media` \| `alta` |
| `valorAnteriorCm`, `valorActualCm` | double \| null | nulos en `OrdenMantenimiento` |
| `timestamp` | long | epoch ms del evento que originó la alerta, o de la orden |
| `origen` | string | `alertas-service` \| `operador-manual` |

!!! warning "Origen de las alertas"
    Las `AlertaOperativa` se derivan de lecturas reales; las `OrdenMantenimiento` son simuladas por un operador (comando manual). No se generan eventos aleatorios.
