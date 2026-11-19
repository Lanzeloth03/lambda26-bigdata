"""S12 - Perfil AIRE: Open-Meteo (CAMS) -> Kafka `atmos-aire` -> Spark Structured Streaming.
Dos consultas sobre el mismo stream:
  * ventanas : ventana fija de 6 h + watermark de 2 h (append): n, PM2.5 prom/max, horas sobre el umbral relativo.
  * inferencia (foreachBatch): predice PM2.5 de la PRÓXIMA hora con el modelo MLlib, la compara cuando llega el dato real
    y mantiene MAE(modelo) vs MAE(persistencia). Si PM2.5 > p95 de entrenamiento emite `aire.episodio` a `atmos-negocio`.
Métricas -> Pushgateway (prefijo lambda26_aire_). Fuentes: --fuente kafka | archivos (JSON-lines, para pruebas).
"""
import argparse, json, os, sys, threading, time, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aire_comun import ESQUEMA, FEATURES, HORA_MS, features

P = "lambda26_aire_"
a = argparse.ArgumentParser()
a.add_argument("--fuente", choices=["kafka", "archivos"], default="kafka")
a.add_argument("--bootstrap", default="kafka:9092"); a.add_argument("--topic", default="atmos-aire")
a.add_argument("--topic-negocio", default="atmos-negocio"); a.add_argument("--entrada", help="dir con .json (fuente archivos)")
a.add_argument("--modelo", required=True); a.add_argument("--salida", default="/tmp/s12/aire")
a.add_argument("--job", default="lambda26_aire", help="job de Pushgateway (las corridas de integración usan otro para no pisar el tablero)")
a.add_argument("--pushgateway", default="http://pushgateway:9091"); a.add_argument("--starting", default="earliest")
a.add_argument("--ventana", default="6 hours"); a.add_argument("--watermark", default="2 hours")
a.add_argument("--trigger", default="5 seconds"); a.add_argument("--limpiar", action="store_true")
a.add_argument("--una-vez", action="store_true", help="availableNow: procesa lo pendiente y termina")
A = a.parse_args()
JOB = A.job

import shutil
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel

if A.limpiar: shutil.rmtree(A.salida, ignore_errors=True)
spark = (SparkSession.builder.appName("s12_aire").config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
modelo = PipelineModel.load(A.modelo)
meta = json.load(open(os.path.join(A.modelo, "meta.json")))
UMBRAL = float(meta["umbral_relativo_p95"])

# ---------------- Pushgateway ----------------
def empujar(inst, d):
    txt = "".join(f"# TYPE {P}{k} gauge\n{P}{k} {float(v)}\n" for k, v in d.items() if v is not None)
    try:
        rq = urllib.request.Request(f"{A.pushgateway}/metrics/job/{JOB}/instance/{inst}", data=txt.encode(), method="PUT")
        urllib.request.urlopen(rq, timeout=3).read()
    except Exception as e:  # el job nunca se cae por observabilidad
        print("pushgateway:", e, flush=True)

# ---------------- fuente ----------------
if A.fuente == "kafka":
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", A.bootstrap)
           .option("subscribe", A.topic).option("startingOffsets", A.starting).option("failOnDataLoss", "false").load()
           .select(F.col("value").cast("string").alias("j"), F.col("timestamp").alias("t_kafka")))
    ev = raw.select(F.from_json("j", ESQUEMA).alias("e"), "t_kafka").select("e.*", "t_kafka")
else:
    ev = spark.readStream.schema(ESQUEMA).json(A.entrada).withColumn("t_kafka", F.current_timestamp())
ev = (ev.where("tipoEvento = 'aire.lectura' AND pm25 IS NOT NULL AND pm25 >= 0")
        .withColumn("t_evento", (F.col("timestamp") / 1000).cast("timestamp")))

trig = {"availableNow": True} if A.una_vez else {"processingTime": A.trigger}

# ---------------- listener: watermark y filas descartadas (recibe TODOS los lotes, sin carreras) ----------------
from pyspark.sql.streaming import StreamingQueryListener
vistos, desc = set(), [0]
class Oyente(StreamingQueryListener):
    def onQueryStarted(self, event): pass
    def onQueryIdle(self, event): pass
    def onQueryTerminated(self, event): pass
    def onQueryProgress(self, event):
        try:
            pr = event.progress
            if pr.name != "aire_ventanas" or pr.batchId in vistos: return
            vistos.add(pr.batchId)
            so = pr.stateOperators[0] if pr.stateOperators else None
            desc[0] += (so.numRowsDroppedByWatermark if so else 0) or 0
            wm = (pr.eventTime or {}).get("watermark")
            empujar("streaming", {"filas_descartadas_watermark_total": desc[0], "estado_filas": so.numRowsTotal if so else 0,
                                  "input_rows_per_second": pr.inputRowsPerSecond or 0, "lote_ventanas": pr.batchId,
                                  "watermark_seconds": (time.mktime(time.strptime(wm[:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone) if wm else 0})
        except Exception as e:
            print("listener:", e, flush=True)
spark.streams.addListener(Oyente())

# ---------------- consulta 1: ventanas + watermark ----------------
vent = (ev.withWatermark("t_evento", A.watermark)
          .groupBy(F.window("t_evento", A.ventana))
          .agg(F.count("*").alias("n"), F.round(F.avg("pm25"), 2).alias("pm25_prom"), F.max("pm25").alias("pm25_max"),
               F.round(F.avg("pm10"), 2).alias("pm10_prom"),
               F.sum(F.when(F.col("pm25") > UMBRAL, 1).otherwise(0)).alias("horas_sobre_umbral"))
          .select(F.col("window.start").alias("ini"), F.col("window.end").alias("fin"), "n", "pm25_prom", "pm25_max", "pm10_prom", "horas_sobre_umbral"))

def lote_ventanas(df, bid):
    df.write.mode("append").parquet(f"{A.salida}/ventanas")
    u = df.orderBy(F.col("fin").desc()).limit(1).collect()
    if u:
        r = u[0]
        empujar("ventanas", {"ventana_n": r["n"], "ventana_pm25_prom": r["pm25_prom"], "ventana_pm25_max": r["pm25_max"],
                             "ventana_horas_sobre_umbral": r["horas_sobre_umbral"], "ventana_fin_seconds": r["fin"].timestamp()})

q1 = (vent.writeStream.outputMode("append").foreachBatch(lote_ventanas).trigger(**trig)
      .option("checkpointLocation", f"{A.salida}/_ckpt/ventanas").queryName("aire_ventanas").start())

# ---------------- consulta 2: inferencia a +1 h ----------------
hist, pend = {}, {}                       # hist: ts->(pm25,pm10);  pend: ts_objetivo->(pred, l0)
acc = dict(n=0, e_mod=0.0, e_per=0.0, eventos=0, episodios=0, ultimo=-1)

def lote_inferencia(df, bid):
    if bid <= acc["ultimo"]: return       # idempotente ante reintento del mismo lote
    acc["ultimo"] = bid
    # el dato "demo-tardio" es una prueba del watermark: no se usa para inferir
    filas = sorted(df.where("origen <> 'demo-tardio'").select("timestamp", "pm25", "pm10").collect(), key=lambda r: r[0])
    nuevas, evaluadas, episodios = [], [], []
    for ts, pm25, pm10 in filas:
        hist[ts] = (pm25, pm10); acc["eventos"] += 1
        if pm25 > UMBRAL: episodios.append((ts, pm25))
    for ts, _, _ in filas:                # features solo usan horas <= ts: sin fuga de información
        f = features(hist, ts)
        if f: nuevas.append((ts, f))
    pred_ult = None
    if nuevas:                            # 1) predecir la hora siguiente de cada fila nueva
        pdf = spark.createDataFrame([{**f, "ts": ts} for ts, f in nuevas])
        for r in modelo.transform(pdf).select("ts", "l0", "prediction").collect():
            pend[r["ts"] + HORA_MS] = (r["l0"] + r["prediction"], r["l0"])
        pred_ult = pend[nuevas[-1][0] + HORA_MS][0]
    for ts, pm25, _ in filas:             # 2) evaluar las horas cuya predicción ya existía
        if ts in pend:
            pred, l0 = pend.pop(ts)
            acc["n"] += 1; acc["e_mod"] += abs(pred - pm25); acc["e_per"] += abs(l0 - pm25)
            evaluadas.append((ts, pred, l0, pm25))
    if evaluadas:
        spark.createDataFrame(evaluadas, "ts_objetivo LONG, pred DOUBLE, persist DOUBLE, real DOUBLE") \
             .write.mode("append").parquet(f"{A.salida}/evaluacion")
    if episodios and A.fuente == "kafka":
        ej = [json.dumps({"tipoEvento": "aire.episodio", "sensorId": "openmeteo-cams-juliaca", "severidad": "media",
                          "regla": "pm25_sobre_p95_relativo", "pm25": v, "umbral": UMBRAL, "timestamp": t}) for t, v in episodios]
        (spark.createDataFrame([(x,) for x in ej], "value STRING").write.format("kafka")
         .option("kafka.bootstrap.servers", A.bootstrap).option("topic", A.topic_negocio).save())
    acc["episodios"] += len(episodios)
    if filas:
        mm = acc["e_mod"] / acc["n"] if acc["n"] else None; mp = acc["e_per"] / acc["n"] if acc["n"] else None
        ult = filas[-1]
        empujar("inferencia", {"pm25_ultimo": ult[1], "pm10_ultimo": ult[2], "pred_pm25_proxima_hora": pred_ult,
                               "mae_modelo": mm, "mae_persistencia": mp,
                               "mejora_pct": (100 * (mp - mm) / mp) if mp else None, "n_evaluadas": acc["n"],
                               "eventos_total": acc["eventos"], "episodios_total": acc["episodios"], "umbral_p95": UMBRAL,
                               "lote_id": bid, "ultima_hora_evento_seconds": ult[0] / 1000})

q2 = (ev.writeStream.foreachBatch(lote_inferencia).trigger(**trig)
      .option("checkpointLocation", f"{A.salida}/_ckpt/inferencia").queryName("aire_inferencia").start())

try:
    if A.una_vez:
        q1.awaitTermination(); q2.awaitTermination(); time.sleep(3)
        lp = q1.lastProgress or {}
        print(json.dumps({"mae_modelo": acc["e_mod"] / max(acc["n"], 1), "mae_persistencia": acc["e_per"] / max(acc["n"], 1),
                          "n_evaluadas": acc["n"], "eventos": acc["eventos"], "episodios": acc["episodios"],
                          "descartadas_watermark_total": desc[0],
                          "watermark": (lp.get("eventTime") or {}).get("watermark")}))
    else:
        spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    for q in spark.streams.active: q.stop()
