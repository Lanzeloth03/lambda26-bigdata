"""Evalua el modelo GBT guardado (entrenado con 2023) sobre otros anios REALES de Open-Meteo (CAMS, Juliaca).
Compara contra la PERSISTENCIA con el mismo criterio de entrenamiento: delta = pm25(t+1) - pm25(t).
Uso: python evaluar_anios.py [2024 2025 2026]   ->  /tmp/s12/eval_anios.json
"""
import sys, os, json, datetime, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aire_comun import leer_json, filas_entrenamiento

anios = sys.argv[1:] or ["2024", "2025", "2026"]
URL = ("https://air-quality-api.open-meteo.com/v1/air-quality?latitude=-15.5&longitude=-70.13"
       "&hourly=pm10,pm2_5&start_date={a}-01-01&end_date={fin}&timezone=GMT")

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel
spark = (SparkSession.builder.appName("evaluar_anios").master("local[2]")
         .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
modelo = PipelineModel.load("/tmp/s12/modelo_pm25")
hoy = datetime.datetime.utcnow().date()
out = {"generado_utc": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M"), "anios": {}}

for a in anios:
    ruta = f"/tmp/s12/aq_juliaca_{a}.json"
    fin = min(datetime.date(int(a), 12, 31), hoy)
    if a == str(hoy.year) or not os.path.exists(ruta):
        open(ruta, "wb").write(urllib.request.urlopen(URL.format(a=a, fin=fin), timeout=120).read())
    crudo = json.load(open(ruta, encoding="utf-8"))["hourly"]
    total = len(crudo["time"])
    serie = leer_json(ruta)
    info = {"horas_en_respuesta": total, "horas_con_dato": len(serie)}
    if serie:
        iso = lambda ms: datetime.datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%dT%H:%M")
        info.update(primera_hora=iso(serie[0][0]), ultima_hora=iso(serie[-1][0]),
                    pm25_media=round(sum(s[1] for s in serie) / len(serie), 2),
                    pm25_max=max(s[1] for s in serie))
    filas = filas_entrenamiento(serie)
    if len(filas) < 48:
        info["nota"] = "datos insuficientes para evaluar"
        out["anios"][a] = info
        continue
    pred = modelo.transform(spark.createDataFrame(filas)).withColumn(
        "mes", F.date_format(F.from_unixtime(F.col("ts") / 1000), "yyyy-MM"))
    pred = pred.withColumn("e_mod", F.abs(F.col("prediction") - F.col("delta"))).withColumn("e_per", F.abs(F.col("delta")))
    g = pred.agg(F.count("*").alias("n"), F.avg("e_mod").alias("m"), F.avg("e_per").alias("p")).first()
    info.update(horas_evaluadas=g["n"], mae_modelo=round(g["m"], 4), mae_persistencia=round(g["p"], 4),
                mejora_pct=round(100 * (g["p"] - g["m"]) / g["p"], 1))
    meses = pred.groupBy("mes").agg(F.count("*").alias("n"), F.avg("e_mod").alias("m"), F.avg("e_per").alias("p")).orderBy("mes").collect()
    info["por_mes"] = [{"mes": r["mes"], "n": r["n"], "mae_modelo": round(r["m"], 3), "mae_persistencia": round(r["p"], 3),
                        "mejora_pct": round(100 * (r["p"] - r["m"]) / r["p"], 1)} for r in meses]
    out["anios"][a] = info
    print(a, json.dumps({k: v for k, v in info.items() if k != "por_mes"}))

json.dump(out, open("/tmp/s12/eval_anios.json", "w"), indent=2, ensure_ascii=False)
print("LISTO /tmp/s12/eval_anios.json")
spark.stop()
