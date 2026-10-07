import json
import os
import random
import time

from kafka import KafkaProducer


TOPIC_ATMOS = os.getenv("KAFKA_TOPIC_ATMOS", "atmos-eventos")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INTERVAL_MS = int(os.getenv("SENSOR_INTERVAL_MS", "3000"))
SENSOR_IDS = os.getenv(
    "SENSOR_IDS", "esp32-patio,esp32-invernadero,esp32-laboratorio"
).split(",")

RANGOS = {
    "temperatura": (5.0, 40.0),
    "humedad": (20.0, 95.0),
    "presion": (995.0, 1025.0),
}
PASO_MAXIMO = {
    "temperatura": 0.3,
    "humedad": 1.0,
    "presion": 0.5,
}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print(json.dumps({
    "service": "uso-atmos",
    "component": "producer",
    "bootstrapServers": BOOTSTRAP_SERVERS,
    "sensorIds": SENSOR_IDS,
    "intervalMs": INTERVAL_MS,
    "status": "connected",
}))

# Estado inicial de cada sensor — un ESP32 real no salta de golpe entre
# lecturas; cada ronda avanza un poco desde el valor anterior (caminata
# aleatoria acotada), no un número independiente y sin relación con el previo.
estado = {
    sensor_id: {
        variable: round(random.uniform(*rango), 1)
        for variable, rango in RANGOS.items()
    }
    for sensor_id in SENSOR_IDS
}


def siguiente_valor(valor_actual, variable):
    minimo, maximo = RANGOS[variable]
    paso = PASO_MAXIMO[variable]
    nuevo = valor_actual + random.uniform(-paso, paso)
    return round(min(max(nuevo, minimo), maximo), 1)


while True:
    for sensor_id in SENSOR_IDS:
        lectura = estado[sensor_id]
        for variable in RANGOS:
            lectura[variable] = siguiente_valor(lectura[variable], variable)

        data = {
            "tipoEvento": "sensor.lectura",
            "sensorId": sensor_id,
            "temperatura": lectura["temperatura"],
            "humedad": lectura["humedad"],
            "presion": lectura["presion"],
            "origen": "uso-atmos",
            "timestamp": int(time.time() * 1000),
        }

        metadata = producer.send(TOPIC_ATMOS, key=sensor_id, value=data).get(timeout=10)

        log = {
            "service": "uso-atmos",
            "component": "producer",
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "eventType": data["tipoEvento"],
            "sensorId": sensor_id,
            "timestamp": data["timestamp"],
            "status": "published",
        }

        print(json.dumps(log))

    time.sleep(INTERVAL_MS / 1000)
