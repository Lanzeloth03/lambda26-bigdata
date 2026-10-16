# Puente MQTT (test.mosquitto.org) -> Kafka (tema atmos-real)
import os, sys, json, time, logging
import paho.mqtt.client as mqtt
from confluent_kafka import Producer

MQTT_HOST = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "lambda26/atmos-real/lanzeloth/lecturas")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "atmos-real")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("/tmp/bridge.log"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("bridge")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 50})
contador = {"ok": 0, "err": 0}

def entregado(err, msg):
    if err is not None:
        contador["err"] += 1
        log.error("Kafka error: %s", err)
    else:
        contador["ok"] += 1
        if contador["ok"] % 10 == 1:
            log.info("Entregados a Kafka: %d (partición %d, offset %d)",
                     contador["ok"], msg.partition(), msg.offset())

def on_connect(client, userdata, flags, reason_code, properties=None):
    log.info("MQTT conectado (%s). Suscribiendo a %s", reason_code, MQTT_TOPIC)
    client.subscribe(MQTT_TOPIC, qos=0)

def on_message(client, userdata, msg):
    try:
        datos = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        log.warning("Mensaje no JSON, se descarta: %s (%s)", msg.payload[:80], e)
        return
    clave = str(datos.get("sensorId", "desconocido"))
    producer.produce(KAFKA_TOPIC, key=clave, value=msg.payload, callback=entregado)
    producer.poll(0)

cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                      client_id="bridge-lambda26-" + str(int(time.time())))
cliente.on_connect = on_connect
cliente.on_message = on_message
cliente.reconnect_delay_set(min_delay=1, max_delay=30)

log.info("Arrancando puente %s -> %s (%s)", MQTT_TOPIC, KAFKA_TOPIC, KAFKA_BOOTSTRAP)
cliente.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
try:
    cliente.loop_forever(retry_first_connection=True)
finally:
    producer.flush(5)