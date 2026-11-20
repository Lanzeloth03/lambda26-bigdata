#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reentrenamiento del modelo de series de tiempo (S10) sobre el regimen RECIENTE.  |  Lanzeloth Vargas Marichi

Por que existe: el modelo LR de S10 se entreno con un regimen (~118.6 cm). Cuando el sensor cambia de
regimen (deriva), el MAE en produccion se dispara frente a la persistencia (S11: 19.97 vs 1.25 cm).
Este script reentrena con las ultimas N lecturas, valida con split CRONOLOGICO contra la persistencia
y guarda el modelo nuevo SOLO si no es peor que la persistencia (o con --forzar).

  # desde Kafka (lee el topic completo en batch y se queda con las ultimas N validas)
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /tmp/s12/reentrenar_lr.py \
      --salida /tmp/s12/modelo_distancia_lr_v2 --ultimos 600
  # desde el Parquet que escribe el job (columnas fechaHora, valor)
  spark-submit /tmp/s12/reentrenar_lr.py --fuente parquet --ruta /tmp/s12/artifacts/predicciones --salida ...
"""
import argparse
import sys

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession, Window, functions as F

ESQUEMA = "tipoEvento STRING, sensorId STRING, distanciaCm DOUBLE, origen STRING, timestamp LONG"

a = argparse.ArgumentParser()
a.add_argument("--fuente", choices=["kafka", "parquet"], default="kafka")
a.add_argument("--bootstrap", default="kafka:9092")
a.add_argument("--topic", default="atmos-real")
a.add_argument("--ruta", default="", help="solo --fuente parquet")
a.add_argument("--ultimos", type=int, default=600, help="ultimas N lecturas validas (600 ~ 30 min a 3 s)")
a.add_argument("--salida", required=True)
a.add_argument("--forzar", action="store_true")
x = a.parse_args()

spark = SparkSession.builder.appName("S12-reentrenar-lr").config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if x.fuente == "kafka":
    raw = (spark.read.format("kafka").option("kafka.bootstrap.servers", x.bootstrap)
           .option("subscribe", x.topic).option("startingOffsets", "earliest").option("endingOffsets", "latest").load())
    ev = (raw.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("e")).select("e.*")
          .filter(F.col("distanciaCm").isNotNull() & F.col("timestamp").isNotNull()
                  & (F.col("distanciaCm") >= 2) & (F.col("distanciaCm") <= 400)
                  & (F.col("origen").isNull() | (F.col("origen") != "demo-tardio")))
          .select((F.col("timestamp") / 1000).cast("timestamp").alias("tiempo"), F.col("distanciaCm").alias("valor")))
else:
    ev = spark.read.parquet(x.ruta).select(F.col("fechaHora").alias("tiempo"), "valor")

recientes = ev.orderBy(F.desc("tiempo")).limit(x.ultimos)
sup = (recientes.withColumn("siguiente", F.lead("valor", 1).over(Window.orderBy("tiempo")))
       .na.drop(subset=["siguiente"]).withColumn("rn", F.row_number().over(Window.orderBy("tiempo"))).cache())
n = sup.count()
if n < 50:
    sys.exit(f"Muy pocas filas ({n}). Deja correr el sensor mas tiempo.")
corte = int(n * 0.8)
train, test = sup.filter(F.col("rn") <= corte), sup.filter(F.col("rn") > corte)
print(f"filas={n} train={train.count()} test={test.count()} (split cronologico)")

pipe = Pipeline(stages=[VectorAssembler(inputCols=["valor"], outputCol="features"),
                        LinearRegression(featuresCol="features", labelCol="siguiente")])
modelo = pipe.fit(train)
lr = modelo.stages[-1]
print(f"LR reentrenado: siguiente = {lr.coefficients[0]:.4f} * valor + {lr.intercept:.4f}")


def met(df, col, m):
    return RegressionEvaluator(labelCol="siguiente", predictionCol=col, metricName=m).evaluate(df)


pers = test.withColumn("p", F.col("valor"))
pred = modelo.transform(test)
r = {m: (met(pers, "p", m), met(pred, "prediction", m)) for m in ("rmse", "mae")}
print(f"{'':12s}{'persistencia':>14s}{'LR nuevo':>12s}")
for m, (p, l) in r.items():
    print(f"{m.upper():12s}{p:14.4f}{l:12.4f}")
mejora = 100 * (r["rmse"][0] - r["rmse"][1]) / r["rmse"][0] if r["rmse"][0] else 0.0
print(f"Mejora RMSE vs persistencia: {mejora:+.2f}%")

if mejora >= 0 or x.forzar:
    modelo.write().overwrite().save(x.salida)
    print(f"Modelo guardado en {x.salida}")
else:
    print("NO se guarda: el LR no supera a la persistencia en el holdout (use --forzar para guardarlo igual).")
spark.stop()
