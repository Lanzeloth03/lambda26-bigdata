"""Diagnostico: por que el modelo no le gana a la persistencia en la ventana en vivo.
Evalua modelo vs persistencia sobre las ultimas horas reales de la API, y compara con 2023 (prueba) y con 2026 completo."""
import sys, json, re, urllib.request
sys.path.insert(0, "/tmp/s12/scripts")
import aire_comun as aire, openmeteo_a_kafka as pub
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
modelo = PipelineModel.load("/tmp/s12/modelo_pm25")
txt = urllib.request.urlopen("http://pushgateway:9091/metrics").read().decode()
print("PUSHGATEWAY:", {m.group(1): round(float(m.group(2)), 4) for m in re.finditer(r'^lambda26_aire_(n_evaluadas|mae_modelo|mae_persistencia|mejora_pct)\{[^}]*\}\s+(\S+)', txt, re.M)})
DIAS = int(sys.argv[1]) if len(sys.argv) > 1 else 2
pub.API = pub.API.replace("past_days=2", f"past_days={DIAS}")
serie = pub.desde_api(); print("dias pedidos a la API:", DIAS, "· horas recibidas:", len(serie)); hist = {t: (a, b) for t, a, b in serie}
filas = []
for t, a, _ in serie:
    f = aire.features(hist, t); nxt = hist.get(t + aire.HORA_MS)
    if f and nxt: filas.append({"ts": t, **f, "real_sig": nxt[0], "pm25": a})
df = spark.createDataFrame(filas)
p = [(r["ts"], r["pm25"], r["pm25"] + r["prediction"], r["real_sig"]) for r in modelo.transform(df).select("ts", "pm25", "prediction", "real_sig").collect()]
p.sort()
def resumen(nombre, xs):
    if not xs: return
    em = sum(abs(pr - re_) for _, _, pr, re_ in xs) / len(xs); ep = sum(abs(a - re_) for _, a, _, re_ in xs) / len(xs)
    sesgo = sum(pr - re_ for _, _, pr, re_ in xs) / len(xs)
    print(f"{nombre}: n={len(xs)} MAE modelo={em:.3f} persistencia={ep:.3f} mejora={100*(ep-em)/ep:+.1f}% sesgo modelo={sesgo:+.3f}")
resumen("Ventana API completa (horas con 24 h de historia)", p)
resumen("Ultimas 24 h", p[-24:]); resumen("Ultimas 12 h", p[-12:])
for d in (7, 14, 30, 60):
    resumen(f"Ultimos {d} dias", p[-24 * d:])
mes = {}
for x in p: mes.setdefault(__import__("datetime").datetime.utcfromtimestamp(x[0] / 1000).strftime("%Y-%m"), []).append(x)
for k, v in sorted(mes.items()): resumen("Mes " + k, v)
print("PM2.5 en la ventana: min %.1f max %.1f media %.2f" % (min(x[1] for x in p), max(x[1] for x in p), sum(x[1] for x in p) / len(p)))
for ts, a, pr, re_ in p[-8:]: print(" ", ts, f"actual={a:.1f} pred={pr:.2f} real_sig={re_:.1f}")
