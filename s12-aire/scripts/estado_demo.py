"""Estado del demo en vivo: procesos, ultimos eventos publicados (con su origen y hora) y metricas del job."""
import os, json, datetime, urllib.request
ahora = datetime.datetime.utcnow()
print("HORA UTC ACTUAL:", ahora.strftime("%Y-%m-%d %H:%M:%S"))
print("\n== PROCESOS ==")
for pid in filter(str.isdigit, os.listdir("/proc")):
    try:
        c = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode(errors="ignore")
    except Exception:
        continue
    if ("openmeteo_a_kafka" in c or "s12_streaming_aire" in c) and "estado_demo" not in c:
        print(pid, c[:230])
for f in ("alimentador.log", "job_aire.log"):
    print(f"\n== ULTIMAS LINEAS {f} ==")
    try:
        print("".join(open("/tmp/s12/" + f, errors="ignore").readlines()[-6:]))
    except Exception as e:
        print("sin archivo", e)
print("== ULTIMOS EVENTOS EN atmos-aire-demo ==")
try:
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers="kafka:9092", consumer_timeout_ms=4000)
    tp = TopicPartition("atmos-aire-demo", 0); c.assign([tp])
    fin = c.end_offsets([tp])[tp]; print("mensajes en el topic:", fin)
    c.seek(tp, max(0, fin - 4))
    for m in c:
        e = json.loads(m.value)
        h = datetime.datetime.utcfromtimestamp(e["timestamp"] / 1000).strftime("%Y-%m-%d %H:%M")
        print(f"offset {m.offset} | hora del dato {h} UTC | pm25 {e['pm25']} | pm10 {e['pm10']} | origen {e['origen']}")
except Exception as ex:
    print("error leyendo Kafka:", ex)
print("\n== METRICAS DEL JOB (Pushgateway) ==")
try:
    t = urllib.request.urlopen("http://pushgateway:9091/metrics", timeout=10).read().decode()
    claves = ("lambda26_aire_eventos_total", "lambda26_aire_n_evaluadas", "lambda26_aire_mae_modelo", "lambda26_aire_mae_persistencia",
              "lambda26_aire_mejora_pct", "lambda26_aire_filas_descartadas_watermark_total", "push_time_seconds{")
    for l in t.splitlines():
        if l.startswith(claves) and "lambda26_aire" in l:
            print(l[:200])
except Exception as ex:
    print("error Pushgateway:", ex)
