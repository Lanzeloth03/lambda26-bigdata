import sys, logging
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                    handlers=[logging.FileHandler("/tmp/consumer.log"), logging.StreamHandler(sys.stdout)])
c = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "atmos-real-group",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
})
c.subscribe(["atmos-real"])
n = 0
while True:
    m = c.poll(1.0)
    if m is None or m.error():
        continue
    n += 1
    if n % 10 == 1:
        logging.info("p%d o%d %s", m.partition(), m.offset(), m.value().decode()[:120])