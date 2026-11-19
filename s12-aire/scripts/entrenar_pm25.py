"""Entrena el modelo de PM2.5 a +1 h con Spark MLlib y lo compara con la PERSISTENCIA (baseline).
Objetivo: delta = pm25(t+1) - pm25(t)  (la predicción es pm25(t) + delta).
Split CRONOLÓGICO 80/20 (nunca aleatorio: es una serie de tiempo). Guarda el modelo solo si le gana a la persistencia en MAE.
Uso: python entrenar_pm25.py --archivo aq_juliaca_2023.json --salida /tmp/s12/modelo_pm25
"""
import argparse, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aire_comun import leer_json, filas_entrenamiento, FEATURES

p = argparse.ArgumentParser()
p.add_argument("--archivo", required=True); p.add_argument("--salida", default="/tmp/s12/modelo_pm25")
p.add_argument("--forzar", action="store_true")
a = p.parse_args()

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression

spark = SparkSession.builder.appName("entrenar_pm25").master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
filas = filas_entrenamiento(leer_json(a.archivo))
corte = int(len(filas) * 0.8)
df = spark.createDataFrame(filas)
tr, te = df.filter(F.col("ts") < filas[corte]["ts"]), df.filter(F.col("ts") >= filas[corte]["ts"])
ens = VectorAssembler(inputCols=FEATURES, outputCol="x")


def mae(d, col): return d.select(F.avg(F.abs(F.col(col) - F.col("delta")))).first()[0]


res = {}
res["persistencia_mae"] = te.select(F.avg(F.abs("delta"))).first()[0]
lin = Pipeline(stages=[ens, LinearRegression(featuresCol="x", labelCol="delta")]).fit(tr)
res["lineal_mae"] = mae(lin.transform(te), "prediction")
gbt = Pipeline(stages=[ens, GBTRegressor(featuresCol="x", labelCol="delta", lossType="absolute",
                                         maxIter=80, maxDepth=4, stepSize=0.1, seed=7)]).fit(tr)
res["gbt_mae"] = mae(gbt.transform(te), "prediction")
res["mejora_vs_persistencia_pct"] = 100 * (res["persistencia_mae"] - res["gbt_mae"]) / res["persistencia_mae"]
p95 = tr.approxQuantile("l0", [0.95], 0.001)[0]
res.update(umbral_relativo_p95=p95, n_train=tr.count(), n_test=te.count(), features=FEATURES,
           test_desde=filas[corte]["ts"], unidad="ug/m3", fuente="Open-Meteo Air Quality (CAMS, modelado, ~45 km)")
print(json.dumps(res, indent=2))
if res["mejora_vs_persistencia_pct"] > 0 or a.forzar:
    gbt.write().overwrite().save(a.salida)
    json.dump(res, open(os.path.join(a.salida, "meta.json"), "w"), indent=2)
    print("modelo guardado en", a.salida)
else:
    print("NO se guarda: no mejora a la persistencia")
spark.stop()
