#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S12 - Job persistente de Spark Structured Streaming (dimension U2)
Autor: Lanzeloth Vargas Marichi

Pipeline:  ESP32 + HC-SR04 -> MQTT -> bridge -> Kafka (atmos-real) -> ESTE JOB -> Pushgateway
           -> Prometheus -> Grafana

El job corre como proceso persistente (spark-submit), NO como notebook, y lanza 3 consultas
con su propio checkpoint:

  1. inferencia : cada evento valido pasa por el modelo LR de S10 (prediccion t+1), se mide el
                  error REAL contra el valor que llega despues (MAE del modelo y de la
                  persistencia), se guarda en Parquet y se publican los KPIs.
  2. ventanas   : ventana tumbling + watermark. Salida en modo append: una ventana solo se emite
                  cuando el watermark pasa su fin. Se guarda en Parquet y se publica la ultima.
  3. negocio    : cuenta los eventos de negocio (topic atmos-negocio, S6) por tipo y severidad.

Un hilo monitor lee lastProgress de cada consulta cada 5 s y publica: filas/s, duracion del
lote, atraso de offsets, watermark, filas de estado y filas DESCARTADAS por el watermark
(evidencia de dato tardio).

Uso (dentro del contenedor lambda26-pyspark):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
      /tmp/s12/s12_streaming_u2.py --modelo <ruta modelo_distancia_lr> --limpiar

Prueba sin Kafka (solo para desarrollo):  --fuente archivos --entrada <carpeta con .json>
"""
import argparse
import os
import shutil
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F

ESQUEMA = "tipoEvento STRING, sensorId STRING, distanciaCm DOUBLE, origen STRING, timestamp LONG"
ESQUEMA_NEGOCIO = ("eventId STRING, tipoEvento STRING, version INT, sensorId STRING, severidad STRING, "
                   "detalle STRING, valorAnteriorCm DOUBLE, valorActualCm DOUBLE, timestamp LONG, origen STRING")
MIN_CM, MAX_CM = 2, 400          # rango fisico valido del HC-SR04
JOB = "lambda26_s12"
P = "lambda26_s12_"              # prefijo de metricas


# ----------------------------------------------------------------------------------------------
# Argumentos
# ----------------------------------------------------------------------------------------------
def leer_argumentos():
    a = argparse.ArgumentParser(description="S12 - job streaming U2")
    a.add_argument("--fuente", choices=["kafka", "archivos"], default="kafka")
    a.add_argument("--bootstrap", default="kafka:9092")
    a.add_argument("--topic", default="atmos-real")
    a.add_argument("--topic-negocio", default="atmos-negocio", help="vacio = desactiva la consulta de negocio")
    a.add_argument("--entrada", default="", help="solo --fuente archivos")
    a.add_argument("--entrada-negocio", default="", help="solo --fuente archivos (prueba de la consulta de negocio)")
    a.add_argument("--modelo", required=True, help="ruta del PipelineModel guardado en S10 (modelo_distancia_lr)")
    a.add_argument("--salida", default="/tmp/s12/artifacts")
    a.add_argument("--pushgateway", default="http://pushgateway:9091", help="vacio = no publica")
    a.add_argument("--starting", default="latest", choices=["latest", "earliest"])
    a.add_argument("--ventana", default="1 minute")
    a.add_argument("--watermark", default="30 seconds")
    a.add_argument("--trigger", default="5 seconds")
    a.add_argument("--limpiar", action="store_true", help="borra salidas y checkpoints antes de arrancar")
    a.add_argument("--una-vez", action="store_true", help="trigger availableNow (prueba/replay) y termina")
    return a.parse_args()


# ----------------------------------------------------------------------------------------------
# Pushgateway (PUT reemplaza el grupo job/instance; cada consulta usa su propia instancia)
# ----------------------------------------------------------------------------------------------
def empujar(url, instancia, metricas):
    """metricas: lista de (nombre, tipo, ayuda, valor, etiquetas|None). Nunca debe tumbar el job."""
    if not url:
        return
    familias = {}
    for nombre, tipo, ayuda, valor, etiq in metricas:
        if valor is None:
            continue
        try:
            v = float(valor)
        except (TypeError, ValueError):
            continue
        if v != v:  # NaN
            continue
        fam = familias.setdefault(nombre, {"tipo": tipo, "ayuda": ayuda, "muestras": []})
        e = ""
        if etiq:
            e = "{" + ",".join(f'{k}="{val}"' for k, val in etiq.items()) + "}"
        fam["muestras"].append(f"{nombre}{e} {v}")
    lineas = []
    for nombre, f in familias.items():
        lineas += [f"# HELP {nombre} {f['ayuda']}", f"# TYPE {nombre} {f['tipo']}"] + f["muestras"]
    cuerpo = ("\n".join(lineas) + "\n").encode("utf-8")
    req = urllib.request.Request(f"{url}/metrics/job/{JOB}/instance/{instancia}", data=cuerpo, method="PUT")
    try:
        urllib.request.urlopen(req, timeout=3).read()
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] Pushgateway ({instancia}): {e}", flush=True)


# ----------------------------------------------------------------------------------------------
# Estado en memoria del proceso (se reinicia si el job se reinicia; el checkpoint conserva offsets)
# ----------------------------------------------------------------------------------------------
class EstadoInferencia:
    lote_max = -1
    n_total = 0
    n_invalidos = 0
    n_pares = 0
    ae_modelo = 0.0
    ae_persistencia = 0.0
    ultimo_valor = None
    ultima_pred = None      # prediccion hecha para el SIGUIENTE valor
    arranque = time.time()


class EstadoVentanas:
    lote_max = -1
    cerradas = 0


class EstadoNegocio:
    lote_max = -1
    conteo = {}             # (tipo, severidad) -> n
    ultimo_unix = 0.0


def iso_a_unix(s):
    if not s:
        return None
    return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


# ----------------------------------------------------------------------------------------------
# Lectura de la fuente
# ----------------------------------------------------------------------------------------------
def leer_fuente(spark, a, topic, carpeta=None):
    if a.fuente == "kafka":
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", a.bootstrap)
                .option("subscribe", topic)
                .option("startingOffsets", a.starting)
                .option("failOnDataLoss", "false")
                .load()
                .select(F.col("offset").cast("long").alias("offset"), F.col("value").cast("string").alias("value")))
    return (spark.readStream.option("maxFilesPerTrigger", 1).text(carpeta or a.entrada)
            .select(F.lit(0).cast("long").alias("offset"), F.col("value")))


def main():
    a = leer_argumentos()
    if a.limpiar and os.path.exists(a.salida):
        shutil.rmtree(a.salida, ignore_errors=True)
    os.makedirs(a.salida, exist_ok=True)
    chk = lambda n: os.path.join(a.salida, f"chk_{n}")      # noqa: E731
    sal = lambda n: os.path.join(a.salida, n)               # noqa: E731

    spark = (SparkSession.builder.appName("S12-streaming-U2")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    modelo = PipelineModel.load(a.modelo)

    print("=" * 78)
    print(f"S12 streaming U2 | Spark {spark.version} | fuente={a.fuente} topic={a.topic} starting={a.starting}")
    print(f"ventana={a.ventana} watermark={a.watermark} trigger={a.trigger}")
    print(f"modelo={a.modelo} etapas={[type(s).__name__ for s in modelo.stages]}")
    print(f"salida={a.salida}  pushgateway={a.pushgateway or '(desactivado)'}")
    print("=" * 78, flush=True)

    # ---------------- base comun: parseo + validacion + campos de observabilidad ----------------
    base = leer_fuente(spark, a, a.topic)
    ev = (base.select("offset", F.from_json("value", ESQUEMA).alias("e")).select("offset", "e.*")
          .withColumn("valor", F.col("distanciaCm"))
          .withColumn("isValid", F.col("tipoEvento").isNotNull() & F.col("sensorId").isNotNull()
                      & F.col("valor").isNotNull() & F.col("timestamp").isNotNull()
                      & (F.col("valor") >= MIN_CM) & (F.col("valor") <= MAX_CM))
          .withColumn("tiempoEvento", (F.col("timestamp") / 1000).cast("timestamp"))
          .withColumn("latencyMs", F.unix_millis(F.current_timestamp()) - F.col("timestamp")))

    # ============================ CONSULTA 1: inferencia por evento ============================
    def procesar_inferencia(df, batch_id):
        S = EstadoInferencia
        if batch_id <= S.lote_max:
            print(f"[inferencia] lote {batch_id} repetido: se omite (idempotencia)", flush=True)
            return
        S.lote_max = batch_id
        t0 = time.time()
        df.persist()
        n_lote = df.count()
        if n_lote == 0:
            df.unpersist()
            return
        # los eventos de demostracion (origen demo-tardio) no deben contaminar el MAE de produccion
        validos = (df.filter("isValid AND (origen IS NULL OR origen <> 'demo-tardio')")
                   .select("offset", "timestamp", "valor", "latencyMs"))
        pdf = (modelo.transform(validos)
               .select("offset", "timestamp", "valor", "latencyMs", F.col("prediction").alias("pred"))
               .orderBy("timestamp", "offset").toPandas())
        df.unpersist()
        S.n_invalidos += n_lote - len(pdf)
        if pdf.empty:
            empujar(a.pushgateway, "inferencia_invalidos", [
                (P + "eventos_invalidos_total", "counter", "Eventos descartados por validacion", S.n_invalidos, None)])
            print(f"[inferencia] lote {batch_id}: {n_lote} evento(s), todos invalidos (acumulado {S.n_invalidos})",
                  flush=True)
            return

        pred_alineada, err = [], []
        for _, r in pdf.iterrows():
            if S.ultima_pred is not None:
                e_m = abs(r.valor - S.ultima_pred)
                e_p = abs(r.valor - S.ultimo_valor)
                S.ae_modelo += e_m
                S.ae_persistencia += e_p
                S.n_pares += 1
                pred_alineada.append(S.ultima_pred)
                err.append(e_m)
            else:
                pred_alineada.append(None)
                err.append(None)
            S.ultimo_valor, S.ultima_pred = float(r.valor), float(r.pred)
        S.n_total += len(pdf)

        out = pd.DataFrame({
            "batchId": int(batch_id),
            "id": pdf.offset.astype("int64"),
            "fechaHora": pd.to_datetime(pdf.timestamp, unit="ms"),
            "valor": pdf.valor.astype("float64"),
            "prediccionSiguiente": pdf.pred.astype("float64"),
            "prediccionAlineada": pd.Series(pred_alineada, dtype="float64"),
            "errorAbs": pd.Series(err, dtype="float64"),
            "latencyMs": pdf.latencyMs.astype("int64"),
        })
        spark.createDataFrame(out).write.mode("append").parquet(sal("predicciones"))

        mae_m = S.ae_modelo / S.n_pares if S.n_pares else None
        mae_p = S.ae_persistencia / S.n_pares if S.n_pares else None
        ultimo_err = err[-1]
        empujar(a.pushgateway, "inferencia", [
            (P + "eventos_procesados_total", "counter", "Eventos validos procesados por el job", S.n_total, None),
            (P + "eventos_invalidos_total", "counter", "Eventos invalidos o de demostracion excluidos de la inferencia", S.n_invalidos, None),
            (P + "valor_actual_cm", "gauge", "Ultima distanciaCm real", S.ultimo_valor, None),
            (P + "prediccion_siguiente_cm", "gauge", "Prediccion LR para t+1", S.ultima_pred, None),
            (P + "prediccion_alineada_cm", "gauge", "Prediccion que se hizo para el valor actual",
             pred_alineada[-1], None),
            (P + "error_abs_ultimo_cm", "gauge", "Error absoluto del ultimo par prediccion/real", ultimo_err, None),
            (P + "mae_modelo_cm", "gauge", "MAE en produccion del modelo LR (desde el arranque)", mae_m, None),
            (P + "mae_persistencia_cm", "gauge", "MAE en produccion de la persistencia", mae_p, None),
            (P + "pares_evaluados_total", "counter", "Pares prediccion/real evaluados", S.n_pares, None),
            (P + "latencia_ms_promedio_lote", "gauge", "Latencia media del lote (ms)", float(pdf.latencyMs.mean()), None),
            (P + "latencia_ms_p95_lote", "gauge", "Latencia p95 del lote (ms)", float(pdf.latencyMs.quantile(0.95)), None),
            (P + "ultimo_offset", "gauge", "Ultimo offset procesado", int(pdf.offset.iloc[-1]), None),
            (P + "lote_filas", "gauge", "Filas validas del ultimo lote", len(pdf), None),
            (P + "lote_duracion_s", "gauge", "Segundos de proceso del ultimo lote", time.time() - t0, None),
            (P + "job_activo_desde_unix", "gauge", "Instante de arranque del job", S.arranque, None),
        ])
        print(f"[inferencia] lote {batch_id}: {len(pdf)} filas | valor={S.ultimo_valor:.1f} "
              f"pred(t+1)={S.ultima_pred:.1f} | MAE modelo="
              f"{'-' if mae_m is None else f'{mae_m:.2f}'} persistencia={'-' if mae_p is None else f'{mae_p:.2f}'}",
              flush=True)

    # ============================ CONSULTA 2: ventana + watermark ==============================
    agg = (ev.filter("isValid").withWatermark("tiempoEvento", a.watermark)
           .groupBy(F.window("tiempoEvento", a.ventana))
           .agg(F.count("*").alias("n"), F.round(F.avg("valor"), 3).alias("promedioCm"),
                F.min("valor").alias("minimoCm"), F.max("valor").alias("maximoCm"),
                F.round(F.avg("latencyMs"), 1).alias("latenciaMediaMs")))

    def procesar_ventanas(df, batch_id):
        S = EstadoVentanas
        if batch_id <= S.lote_max or df.isEmpty():
            return
        S.lote_max = batch_id
        out = df.select(F.col("window.start").alias("inicio"), F.col("window.end").alias("fin"),
                        "n", "promedioCm", "minimoCm", "maximoCm", "latenciaMediaMs",
                        F.lit(int(batch_id)).alias("batchId"))
        out.write.mode("append").parquet(sal("ventanas"))
        ult = (out.select(F.unix_timestamp("inicio").alias("ini"), "n", "promedioCm", "minimoCm", "maximoCm")
               .orderBy(F.desc("ini")).first())
        S.cerradas += out.count()
        empujar(a.pushgateway, "ventanas", [
            (P + "ventana_cerrada_inicio_unix", "gauge", "Inicio de la ultima ventana emitida", ult.ini, None),
            (P + "ventana_n", "gauge", "Eventos en la ultima ventana emitida", ult.n, None),
            (P + "ventana_promedio_cm", "gauge", "Promedio de la ultima ventana emitida", ult.promedioCm, None),
            (P + "ventana_minimo_cm", "gauge", "Minimo de la ultima ventana emitida", ult.minimoCm, None),
            (P + "ventana_maximo_cm", "gauge", "Maximo de la ultima ventana emitida", ult.maximoCm, None),
            (P + "ventanas_cerradas_total", "counter", "Ventanas emitidas desde el arranque", S.cerradas, None),
        ])
        print(f"[ventanas] lote {batch_id}: {out.count()} ventana(s) cerrada(s); ultima n={ult.n} "
              f"prom={ult.promedioCm}", flush=True)

    # ============================ CONSULTA 3: eventos de negocio ===============================
    def procesar_negocio(df, batch_id):
        S = EstadoNegocio
        if batch_id <= S.lote_max or df.isEmpty():
            return
        S.lote_max = batch_id
        pdf = (df.select(F.from_json("value", ESQUEMA_NEGOCIO).alias("e")).select("e.*")
               .filter(F.col("eventId").isNotNull()).toPandas())
        if pdf.empty:
            return
        for (tipo, sev), n in pdf.groupby(["tipoEvento", "severidad"]).size().items():
            S.conteo[(tipo, sev)] = S.conteo.get((tipo, sev), 0) + int(n)
        S.ultimo_unix = max(S.ultimo_unix, float(pdf.timestamp.max()) / 1000.0)
        met = [(P + "eventos_negocio_total", "counter", "Eventos de negocio consumidos por tipo y severidad",
                n, {"tipo": t, "severidad": s}) for (t, s), n in S.conteo.items()]
        met.append((P + "evento_negocio_ultimo_unix", "gauge", "Instante del ultimo evento de negocio",
                    S.ultimo_unix, None))
        empujar(a.pushgateway, "negocio", met)
        print(f"[negocio] lote {batch_id}: +{len(pdf)} evento(s) | acumulado={sum(S.conteo.values())}", flush=True)

    # ---------------------------------- arrancar consultas ---------------------------------------
    trig = {"availableNow": True} if a.una_vez else {"processingTime": a.trigger}
    consultas = [
        ev.writeStream.foreachBatch(procesar_inferencia).queryName("inferencia")
        .option("checkpointLocation", chk("inferencia")).trigger(**trig).start(),
        agg.writeStream.outputMode("append").foreachBatch(procesar_ventanas).queryName("ventanas")
        .option("checkpointLocation", chk("ventanas")).trigger(**trig).start(),
    ]
    if a.topic_negocio and (a.fuente == "kafka" or a.entrada_negocio):
        neg = leer_fuente(spark, a, a.topic_negocio, a.entrada_negocio)
        consultas.append(neg.writeStream.foreachBatch(procesar_negocio).queryName("negocio")
                         .option("checkpointLocation", chk("negocio")).trigger(**trig).start())
    print("Consultas activas:", [q.name for q in consultas], flush=True)

    # ---------------------------------- hilo monitor ---------------------------------------------
    descartadas = {}      # consulta -> filas descartadas por watermark (acumulado)
    ultimo_lote_visto = {}

    def monitor():
        while True:
            time.sleep(5)
            met = []
            for q in list(spark.streams.active):
                try:
                    p = q.lastProgress
                    recientes = list(q.recentProgress)
                except Exception:  # noqa: BLE001
                    continue
                if not p:
                    continue
                # numRowsDroppedByWatermark es POR LOTE: se acumula una sola vez por batchId
                for r in recientes:
                    bid = r.get("batchId", -1)
                    if bid > ultimo_lote_visto.get(q.name, -1):
                        ultimo_lote_visto[q.name] = bid
                        d = sum(int(so.get("numRowsDroppedByWatermark") or 0) for so in r.get("stateOperators", []))
                        if d:
                            descartadas[q.name] = descartadas.get(q.name, 0) + d
                            print(f"[tardios] lote {bid}: {d} agregado(s) tardio(s) descartado(s) por el watermark "
                                  f"(acumulado {descartadas[q.name]})", flush=True)
                e = {"consulta": q.name}
                met += [
                    (P + "lote_actual", "gauge", "batchId del ultimo micro-lote", p.get("batchId"), e),
                    (P + "entrada_filas_por_s", "gauge", "inputRowsPerSecond", p.get("inputRowsPerSecond"), e),
                    (P + "proceso_filas_por_s", "gauge", "processedRowsPerSecond", p.get("processedRowsPerSecond"), e),
                    (P + "lote_duracion_ms", "gauge", "Duracion total del micro-lote (ms)",
                     (p.get("batchDuration") or p.get("durationMs", {}).get("triggerExecution")), e),
                ]
                for s in p.get("sources", []):
                    m = s.get("metrics") or {}
                    met.append((P + "atraso_offsets_promedio", "gauge", "avgOffsetsBehindLatest",
                                m.get("avgOffsetsBehindLatest"), e))
                    met.append((P + "atraso_offsets_max", "gauge", "maxOffsetsBehindLatest",
                                m.get("maxOffsetsBehindLatest"), e))
                wm = (p.get("eventTime") or {}).get("watermark")
                if wm:
                    met.append((P + "watermark_unix", "gauge", "Watermark actual (epoch s)", iso_a_unix(wm), e))
                for so in p.get("stateOperators", []):
                    met.append((P + "estado_filas", "gauge", "numRowsTotal del estado", so.get("numRowsTotal"), e))
                    met.append((P + "filas_descartadas_watermark_total", "counter",
                                "Agregados tardios (filas de estado/ventana) descartados por el watermark, acumulado desde el arranque",
                                descartadas.get(q.name, 0), e))
            met.append((P + "consultas_activas", "gauge", "Consultas Structured Streaming activas",
                        len(spark.streams.active), None))
            empujar(a.pushgateway, "progreso", met)

    threading.Thread(target=monitor, daemon=True).start()

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nDeteniendo consultas (el checkpoint conserva los offsets)...", flush=True)
    finally:
        for q in list(spark.streams.active):
            q.stop()
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
