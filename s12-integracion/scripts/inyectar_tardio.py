#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Inyecta UN evento TARDIO en atmos-real para demostrar el watermark en vivo.
El evento lleva timestamp = ahora - N segundos (default 300 s), mucho mas viejo que el watermark
(30 s), y origen = demo-tardio para que el job lo excluya del MAE de produccion.
Efecto esperado: el job imprime "[tardios] ... descartado(s) por el watermark" y el panel
"Agregados tardios descartados" sube en 1 (Spark cuenta filas de estado/ventana, no eventos sueltos).

Uso (dentro del contenedor):  python /tmp/s12/inyectar_tardio.py [--segundos-atras 300] [--valor 20]
Requiere kafka-python.
"""
import argparse
import json
import sys
import time

a = argparse.ArgumentParser()
a.add_argument("--bootstrap", default="kafka:9092")
a.add_argument("--topic", default="atmos-real")
a.add_argument("--segundos-atras", type=int, default=300)
a.add_argument("--valor", type=float, default=20.0, help="distanciaCm del evento inyectado")
x = a.parse_args()
try:
    from kafka import KafkaProducer
except ImportError:
    sys.exit("Falta kafka-python (o falla en Python 3.12). Prueba:  pip install -U kafka-python   o   pip install kafka-python-ng")

ts = int((time.time() - x.segundos_atras) * 1000)
ev = {"tipoEvento": "sensor.lectura", "sensorId": "esp32-fisico-lanzeloth", "distanciaCm": x.valor,
      "origen": "demo-tardio", "timestamp": ts}
p = KafkaProducer(bootstrap_servers=x.bootstrap, acks="all", key_serializer=lambda s: s.encode(),
                  value_serializer=lambda d: json.dumps(d).encode())
md = p.send(x.topic, key=ev["sensorId"], value=ev).get(timeout=10)
p.flush()
print(f"Evento tardio publicado en {md.topic}[{md.partition}]@{md.offset} con timestamp {x.segundos_atras}s en el pasado")
