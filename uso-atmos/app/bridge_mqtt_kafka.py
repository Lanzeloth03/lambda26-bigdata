import json
import os
import uuid

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


# test.mosquitto.org es un broker PÚBLICO — cualquiera en internet puede
# publicar o suscribirse a cualquier topic. "equipo01" evita que tus mensajes
# se mezclen con los de otro equipo del curso; cámbialo por tu propio ID.
MQTT_HOST = os.getenv("MQTT_HOST", "test.mosquitto.org")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "lambda26/atmos/equipo01/lecturas")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_ATMOS = os.getenv("KAFKA_TOPIC_ATMOS", "atmos-eventos")

# El puente no valida ni transforma el payload — lo reenvía tal cual llegó
# por MQTT. Esquema, rango físico y JSON malformado se validan en un solo
# lugar (consumer_sensores.py), sin duplicar esa lógica aquí.
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: value,
)


def on_connect(client, userdata, flags, reason_code, properties=None):
    print(json.dumps({
        "service": "uso-atmos",
        "component": "bridge",
        "mqttHost": MQTT_HOST,
        "mqttTopic": MQTT_TOPIC,
        "status": "connected" if reason_code == 0 else f"connect_failed:{reason_code}",
    }))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    raw = msg.payload
    try:
        event = json.loads(raw.decode("utf-8"))
        sensor_id = event.get("sensorId") or "desconocido"
    except json.JSONDecodeError:
        sensor_id = "desconocido"

    metadata = producer.send(KAFKA_TOPIC_ATMOS, key=sensor_id, value=raw).get(timeout=10)

    log = {
        "service": "uso-atmos",
        "component": "bridge",
        "mqttTopic": msg.topic,
        "kafkaTopic": metadata.topic,
        "partition": metadata.partition,
        "offset": metadata.offset,
        "sensorId": sensor_id,
        "status": "forwarded",
    }
    print(json.dumps(log))


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"uso-atmos-bridge-{uuid.uuid4().hex[:8]}")
if MQTT_USERNAME and MQTT_PASSWORD:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

print(json.dumps({
    "service": "uso-atmos",
    "component": "bridge",
    "mqttHost": MQTT_HOST,
    "mqttPort": MQTT_PORT,
    "kafkaBootstrapServers": KAFKA_BOOTSTRAP_SERVERS,
    "status": "starting",
}))

client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
client.loop_forever()
