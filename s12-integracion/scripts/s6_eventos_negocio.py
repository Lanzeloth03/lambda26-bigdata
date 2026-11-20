#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S6 - Eventos empresariales en Kafka (dimension U2)  |  Autor: Lanzeloth Vargas Marichi

Servicio de alertas del monitoreo. Publica EVENTOS DE NEGOCIO en el topic `atmos-negocio`
(clave = sensorId) y los consume en vivo. Contrato completo en docs/u2/ingesta.md.

Subcomandos:
  topico    crea el topic atmos-negocio (3 particiones, replicacion 1)
  servicio  consume atmos-real (grupo atmos-alertas) y publica AlertaOperativa cuando una regla se cumple
  orden     publica una OrdenMantenimiento (la emite un operador; evento de negocio simulado)
  consumir  consume atmos-negocio y lo imprime en vivo (el "consumidor" de la demo)

Requiere kafka-python dentro del contenedor lambda26-pyspark (pip install -U kafka-python; si falla en Python 3.12: pip install kafka-python-ng)
"""
import argparse
import json
import sys
import time
import uuid

TOPIC_IN = "atmos-real"
TOPIC_OUT = "atmos-negocio"
MIN_CM, MAX_CM = 2, 400


# ---------------------------- logica pura (probable sin Kafka) ----------------------------
def crear_evento(tipo, sensor, severidad, detalle, anterior, actual, origen, ts_ms=None):
    return {
        "eventId": str(uuid.uuid4()),
        "tipoEvento": tipo,                  # AlertaOperativa | OrdenMantenimiento
        "version": 1,
        "sensorId": sensor,
        "severidad": severidad,              # baja | media | alta
        "detalle": detalle,
        "valorAnteriorCm": anterior,
        "valorActualCm": actual,
        "timestamp": int(ts_ms if ts_ms is not None else time.time() * 1000),
        "origen": origen,
    }


def evaluar(anterior, actual, umbral_salto):
    """Devuelve [(regla, severidad, detalle)] para una lectura. Regla = clave para el enfriamiento."""
    out = []
    if actual < MIN_CM or actual > MAX_CM:
        out.append(("fuera_de_rango", "alta",
                    f"Lectura {actual:.1f} cm fuera del rango fisico del HC-SR04 ({MIN_CM}-{MAX_CM} cm)"))
        return out
    if anterior is not None and abs(actual - anterior) >= umbral_salto:
        sev = "alta" if abs(actual - anterior) >= 50 else "media"
        out.append(("cambio_brusco", sev,
                    f"Cambio brusco de nivel: {anterior:.1f} -> {actual:.1f} cm ({actual - anterior:+.1f} cm en una lectura)"))
    return out


# ---------------------------- Kafka ----------------------------
def kafka():
    try:
        import kafka  # noqa: F401
        return kafka
    except ImportError:
        sys.exit("Falta kafka-python (o falla en Python 3.12). Prueba:  pip install -U kafka-python   o   pip install kafka-python-ng")


def productor(bootstrap):
    k = kafka()
    return k.KafkaProducer(bootstrap_servers=bootstrap, acks="all",
                           key_serializer=lambda s: s.encode("utf-8"),
                           value_serializer=lambda d: json.dumps(d, ensure_ascii=False).encode("utf-8"))


def publicar(prod, ev):
    md = prod.send(TOPIC_OUT, key=ev["sensorId"], value=ev).get(timeout=10)
    print(f"[publicado] {ev['tipoEvento']} sev={ev['severidad']} sensor={ev['sensorId']} "
          f"-> {md.topic}[{md.partition}]@{md.offset} | {ev['detalle']}", flush=True)


def cmd_topico(a):
    k = kafka()
    from kafka.admin import NewTopic
    adm = k.KafkaAdminClient(bootstrap_servers=a.bootstrap)
    try:
        adm.create_topics([NewTopic(TOPIC_OUT, num_partitions=3, replication_factor=1)])
        print(f"Topic {TOPIC_OUT} creado (3 particiones, replicacion 1)")
    except k.errors.TopicAlreadyExistsError:
        print(f"Topic {TOPIC_OUT} ya existe")


def cmd_servicio(a):
    k = kafka()
    cons = k.KafkaConsumer(TOPIC_IN, bootstrap_servers=a.bootstrap, group_id="atmos-alertas",
                           auto_offset_reset="latest", enable_auto_commit=True,
                           value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    prod = productor(a.bootstrap)
    anterior, ultimo_disparo = {}, {}
    print(f"Servicio de alertas activo: {TOPIC_IN} -> {TOPIC_OUT} | salto >= {a.umbral_salto} cm | "
          f"enfriamiento {a.enfriamiento}s", flush=True)
    for msg in cons:
        d = msg.value
        v, sid = d.get("distanciaCm"), d.get("sensorId")
        if v is None or sid is None:
            continue
        for regla, sev, detalle in evaluar(anterior.get(sid), float(v), a.umbral_salto):
            ahora = time.time()
            if ahora - ultimo_disparo.get((sid, regla), 0) < a.enfriamiento:
                continue
            ultimo_disparo[(sid, regla)] = ahora
            publicar(prod, crear_evento("AlertaOperativa", sid, sev, detalle, anterior.get(sid), float(v),
                                        "alertas-service", d.get("timestamp")))
        anterior[sid] = float(v)


def cmd_orden(a):
    prod = productor(a.bootstrap)
    publicar(prod, crear_evento("OrdenMantenimiento", a.sensor, a.severidad, a.motivo, None, None,
                                "operador-manual"))
    prod.flush()


def cmd_consumir(a):
    k = kafka()
    cons = k.KafkaConsumer(TOPIC_OUT, bootstrap_servers=a.bootstrap, group_id=a.grupo,
                           auto_offset_reset="earliest" if a.desde_inicio else "latest",
                           value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    print(f"Consumiendo {TOPIC_OUT} (grupo {a.grupo})... Ctrl+C para salir", flush=True)
    for m in cons:
        e = m.value
        print(f"[consumido] {m.topic}[{m.partition}]@{m.offset} {e.get('tipoEvento')} sev={e.get('severidad')} "
              f"sensor={e.get('sensorId')} id={e.get('eventId')[:8]} | {e.get('detalle')}", flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default="kafka:9092")
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("topico").set_defaults(f=cmd_topico)
    s = sp.add_parser("servicio")
    s.add_argument("--umbral-salto", type=float, default=20.0)
    s.add_argument("--enfriamiento", type=float, default=30.0)
    s.set_defaults(f=cmd_servicio)
    o = sp.add_parser("orden")
    o.add_argument("--sensor", default="esp32-fisico-lanzeloth")
    o.add_argument("--motivo", default="Revision de alineacion del sensor")
    o.add_argument("--severidad", default="baja", choices=["baja", "media", "alta"])
    o.set_defaults(f=cmd_orden)
    c = sp.add_parser("consumir")
    c.add_argument("--grupo", default="atmos-negocio-demo")
    c.add_argument("--desde-inicio", action="store_true")
    c.set_defaults(f=cmd_consumir)
    a = p.parse_args()
    try:
        a.f(a)
    except KeyboardInterrupt:
        print("\nDetenido")


if __name__ == "__main__":
    main()
