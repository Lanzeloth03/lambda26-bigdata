import json
import os
import time

from kafka import KafkaConsumer


TOPIC_ATMOS = os.getenv("KAFKA_TOPIC_ATMOS", "atmos-eventos")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "uso-atmos-group")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Rango físicamente plausible para un sensor real — más ancho que el rango
# simulado del producer (RANGOS en producer_sensores.py), que ya clampea sus
# propios valores. Una lectura fuera de este rango indica un sensor en mal
# estado o un dato corrupto, no una variación climática normal.
RANGO_FISICO = {
    "temperatura": (-20.0, 60.0),
    "humedad": (0.0, 100.0),
    "presion": (900.0, 1100.0),
}


def fuera_de_rango(event):
    for variable, (minimo, maximo) in RANGO_FISICO.items():
        valor = event.get(variable)
        if valor is not None and not (minimo <= valor <= maximo):
            return True
    return False


def deserialize_message(value):
    text = value.decode("utf-8")
    try:
        return {
            "payload": json.loads(text),
            "raw": text,
            "isJson": True,
            "decodeError": None,
        }
    except json.JSONDecodeError as ex:
        return {
            "payload": None,
            "raw": text,
            "isJson": False,
            "decodeError": str(ex),
        }


consumer = KafkaConsumer(
    TOPIC_ATMOS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=deserialize_message,
    key_deserializer=lambda key: key.decode("utf-8") if key is not None else None,
)

print(json.dumps({
    "service": "uso-atmos",
    "component": "consumer",
    "topic": TOPIC_ATMOS,
    "groupId": GROUP_ID,
    "bootstrapServers": BOOTSTRAP_SERVERS,
    "status": "listening",
}))

for msg in consumer:
    decoded = msg.value
    event = decoded["payload"] if decoded["isJson"] else {}
    timestamp = event.get("timestamp")
    processed_at = int(time.time() * 1000)
    latency_ms = processed_at - timestamp if timestamp is not None else None
    is_valid = (
        decoded["isJson"]
        and event.get("tipoEvento") is not None
        and event.get("sensorId") is not None
        and event.get("temperatura") is not None
        and event.get("humedad") is not None
        and event.get("presion") is not None
        and timestamp is not None
    )
    alerta = is_valid and fuera_de_rango(event)

    log = {
        "service": "uso-atmos",
        "component": "consumer",
        "topic": msg.topic,
        "partition": msg.partition,
        "offset": msg.offset,
        "key": msg.key,
        "groupId": GROUP_ID,
        "eventType": event.get("tipoEvento"),
        "sensorId": event.get("sensorId"),
        "temperatura": event.get("temperatura"),
        "humedad": event.get("humedad"),
        "presion": event.get("presion"),
        "timestamp": timestamp,
        "isValid": is_valid,
        "fueraDeRango": alerta,
        "processedAt": processed_at,
        "latencyMs": latency_ms,
        "rawPayload": decoded["raw"],
        "isJson": decoded["isJson"],
        "decodeError": decoded["decodeError"],
        "status": "alerta" if alerta else ("consumed" if is_valid else "invalid"),
    }

    print(json.dumps(log))
