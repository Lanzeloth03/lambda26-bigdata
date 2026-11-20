# Procesamiento streaming con Spark (S8, S10 y S12)

El procesamiento corre como **job persistente** (`spark-submit`, no notebook) en el contenedor `lambda26-pyspark`. Lanza tres consultas, cada una con su **propio checkpoint**.

| Consulta | Qué hace | Salida |
|---|---|---|
| `inferencia` | Valida, aplica el modelo LR a cada evento, mide el error real contra el valor siguiente | Parquet `predicciones/` + métricas |
| `ventanas` | Ventana *tumbling* de 1 min sobre el tiempo del evento + watermark de 30 s, modo `append` | Parquet `ventanas/` + métricas |
| `negocio` | Cuenta eventos de `atmos-negocio` por tipo y severidad | métricas |

## Ventanas

`groupBy(window(tiempoEvento, "1 minute"))` sobre `tiempoEvento = timestamp/1000`. Una ventana completa trae ≈ 20 eventos (1 cada 3 s); la primera y la última son parciales.

## Watermark

`withWatermark("tiempoEvento", "30 seconds")`: el watermark es el máximo tiempo de evento visto menos 30 s.

* En modo `append` una ventana **solo se emite cuando el watermark pasa su fin**, es decir, ≈ 1 min 30 s después de que empieza. Las últimas ventanas permanecen abiertas hasta que llegan eventos nuevos.
* Un evento más viejo que el watermark **se descarta** y el estado de esa ventana ya fue liberado. El job lo registra con `numRowsDroppedByWatermark` (Spark cuenta filas de estado por ventana, no eventos sueltos) y lo muestra en el panel «Agregados tardíos descartados».
* Compromiso: un watermark mayor tolera más retraso (WiFi/MQTT) pero retiene más estado y retrasa la salida.

## Checkpoint y semántica de entrega

El checkpoint guarda los **offsets de Kafka** leídos, el **estado** de las ventanas y el log de **commits** de cada micro-lote.

| Tramo | Garantía |
|---|---|
| Fuente Kafka | al menos una vez (los offsets están en el checkpoint, no en un consumer group) |
| Sink Parquet (S10) | exactamente una vez, por el log de metadatos del sink de archivos |
| `foreachBatch` (S12) | al menos una vez: un lote puede repetirse tras un fallo. El job lo guarda con `batchId` y omite lotes repetidos dentro del mismo proceso |

**Reinicio:** al detener el job y volver a arrancarlo **sin** `--limpiar`, retoma desde el último offset confirmado; los eventos publicados mientras estuvo caído se procesan al volver. El MAE acumulado en memoria vuelve a cero, pero los offsets y el estado de ventanas se conservan.

!!! warning "Un checkpoint pertenece a una consulta y a una fuente"
    Si cambia el topic (como pasó en S9 al pasar de `atmos-eventos` a `atmos-real`) hay que usar un checkpoint nuevo, o Spark interpreta que se perdieron datos.

## Cómo verificarlo en vivo

1. **Ventanas:** log `[ventanas] lote N: …` y Parquet `ventanas/`; en Grafana, «Ventanas emitidas» sube cada minuto.
2. **Watermark:** panel «Watermark: retraso vs reloj» (≈ 30 s + latencia).
3. **Checkpoint:** `ls /tmp/s12/artifacts/chk_inferencia/offsets`, parar el job con Ctrl+C y reiniciarlo: el `batchId` continúa y no hay reproceso.
4. **Dato tardío:** `python /tmp/s12/inyectar_tardio.py` publica un evento con 5 min de retraso; el log muestra `[tardios] …` y el contador del panel sube en 1.

## Validación hecha con replay (S10)

Con `trigger(availableNow=True)` sobre `startingOffsets=earliest`, los eventos guardados en Kafka se reprodujeron para comprobar que el conteo por ventana coincide con el total. Los 30 eventos finales quedaron en dos ventanas todavía abiertas (watermark 16:28:59Z), como corresponde al modo `append`.
