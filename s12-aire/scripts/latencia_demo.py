"""Mide la latencia de punta a punta del demo SIN escribir nada: solo lee.
Para cada evento nuevo del topic atmos-aire-demo toma la hora a la que Kafka lo recibio y calcula
cuanto tarda en reflejarse en el contador lambda26_aire_eventos_total del Pushgateway (lo que lee Grafana).
Uso: python latencia_demo.py [N_EVENTOS=6]"""
import sys, time, re, urllib.request, statistics as st
from kafka import KafkaConsumer, TopicPartition
N = int(sys.argv[1]) if len(sys.argv) > 1 else 6
TOPIC, PGW = "atmos-aire-demo", "http://pushgateway:9091/metrics"

def contador():
    t = urllib.request.urlopen(PGW, timeout=5).read().decode()
    m = re.search(r'^lambda26_aire_eventos_total\{[^}]*job="lambda26_aire"[^}]*\}\s+([0-9.e+]+)', t, re.M) \
        or re.search(r'^lambda26_aire_eventos_total(?:\{[^}]*\})?\s+([0-9.e+]+)', t, re.M)
    return float(m.group(1))

c = KafkaConsumer(bootstrap_servers="kafka:9092", enable_auto_commit=False, group_id=None)
tp = TopicPartition(TOPIC, 0); c.assign([tp]); c.seek_to_end(tp)
base = contador(); pendientes = []; lat = []
print(f"Esperando {N} eventos nuevos (replay: 1 cada ~10 s). Contador inicial: {base:.0f}")
t_fin = time.time() + N * 20 + 30
while len(lat) < N and time.time() < t_fin:
    for lote in c.poll(timeout_ms=200).values():
        for m in lote: pendientes.append(m.timestamp / 1000.0)   # hora de recepcion en Kafka (s)
    try: act = contador()
    except Exception: continue
    ahora = time.time()
    while act > base and pendientes:
        t0 = pendientes.pop(0); base += 1
        lat.append(ahora - t0); print(f"  evento {len(lat)}: {ahora - t0:5.1f} s desde Kafka hasta el contador de Grafana")
    time.sleep(0.25)
if lat:
    print(f"\nLatencia Kafka -> metrica: mediana {st.median(lat):.1f} s | min {min(lat):.1f} s | max {max(lat):.1f} s ({len(lat)} eventos)")
else:
    print("Sin eventos nuevos en la ventana (replay terminado o job detenido).")
