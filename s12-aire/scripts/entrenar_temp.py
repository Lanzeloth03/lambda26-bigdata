"""Entrena el modelo de TEMPERATURA a +1 h con Spark MLlib y lo compara con dos baselines:
  * persistencia:        T(t+1) = T(t)
  * persistencia diaria: T(t+1) = T(t) + [T(t-23) - T(t-24)]   (repite el cambio de ayer; captura el ciclo diurno)
Objetivo: delta = T(t+1) - T(t). Split CRONOLÓGICO 80/20. Guarda el modelo solo si le gana a la persistencia en MAE.
Uso: python entrenar_temp.py --archivo clima_juliaca_2023.json --salida /tmp/s12/modelo_temp
"""
import argparse, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clima_comun import leer_json, filas_entrenamiento, FEATURES

p = argparse.ArgumentParser()
p.add_argument("--archivo", required=True); p.add_argument("--salida", default="/tmp/s12/modelo_temp")
p.add_argument("--forzar", action="store_true")
a = p.parse_args()

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression

spark = SparkSession.builder.appName("entrenar_temp").master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
filas = filas_entrenamiento(leer_json(a.archivo))
corte = int(len(filas) * 0.8)
df = spark.createDataFrame(filas)
tr, te = df.filter(F.col("ts") < filas[corte]["ts"]), df.filter(F.col("ts") >= filas[corte]["ts"])
ens = VectorAssembler(inputCols=FEATURES, outputCol="x")


def mae(d, col): return d.select(F.avg(F.abs(F.col(col) - F.col("delta")))).first()[0]


res = {}
res["persistencia_mae"] = te.select(F.avg(F.abs("delta"))).first()[0]
res["persistencia_diaria_mae"] = mae(te, "delta_diario")
lin = Pipeline(stages=[ens, LinearRegression(featuresCol="x", labelCol="delta")]).fit(tr)
res["lineal_mae"] = mae(lin.transform(te), "prediction")
gbt = Pipeline(stages=[ens, GBTRegressor(featuresCol="x", labelCol="delta", lossType="absolute",
                                         maxIter=80, maxDepth=4, stepSize=0.1, seed=7)]).fit(tr)
res["gbt_mae"] = mae(gbt.transform(te), "prediction")
res["mejora_vs_persistencia_pct"] = 100 * (res["persistencia_mae"] - res["gbt_mae"]) / res["persistencia_mae"]
res["mejora_vs_diaria_pct"] = 100 * (res["persistencia_diaria_mae"] - res["gbt_mae"]) / res["persistencia_diaria_mae"]
res.update(n_train=tr.count(), n_test=te.count(), features=FEATURES, test_desde=filas[corte]["ts"],
           unidad="grados C", fuente="Open-Meteo (temperature_2m, relative_humidity_2m, wind_speed_10m)")
print(json.dumps(res, indent=2))
if res["mejora_vs_persistencia_pct"] > 0 or a.forzar:
    gbt.write().overwrite().save(a.salida)
    json.dump(res, open(os.path.join(a.salida, "meta.json"), "w"), indent=2)
    print("modelo guardado en", a.salida)
else:
    print("NO se guarda: no mejora a la persistencia")
spark.stop()
