"""Piezas compartidas entre entrenamiento y servicio (evita training-serving skew).
Un solo lugar define el contrato del evento y la construcción de features."""
import json, math
from datetime import datetime, timezone

HORA_MS = 3_600_000
ESQUEMA = "tipoEvento STRING, sensorId STRING, pm25 DOUBLE, pm10 DOUBLE, origen STRING, timestamp LONG"
SENSOR_ID = "openmeteo-cams-juliaca"
LAT, LON = -15.5, -70.13
FEATURES = ["l0", "d1", "d3", "d24", "m24", "pm10d", "hs", "hc"]


def a_ms(iso):
    """'2023-01-01T05:00' (GMT) -> epoch ms."""
    return int(datetime.strptime(iso, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc).timestamp() * 1000)


def leer_json(ruta):
    """Devuelve lista ordenada [(ts_ms, pm25, pm10)] descartando huecos (None)."""
    h = json.load(open(ruta, encoding="utf-8"))["hourly"]
    out = [(a_ms(t), a, b) for t, a, b in zip(h["time"], h["pm2_5"], h["pm10"]) if a is not None and b is not None]
    return sorted(out)


def evento(ts, pm25, pm10, origen):
    return {"tipoEvento": "aire.lectura", "sensorId": SENSOR_ID, "pm25": float(pm25),
            "pm10": float(pm10), "origen": origen, "timestamp": int(ts)}


def features(hist, ts):
    """hist: dict ts_ms -> (pm25, pm10). Features de la hora `ts` para predecir ts+1h.
    Devuelve None si falta historia (se necesitan 24 h previas completas)."""
    try:
        y = lambda k: hist[ts - k * HORA_MS][0]
        l0 = y(0)
        m24 = sum(y(k) for k in range(24)) / 24
        hora = ((ts // HORA_MS) + 1) % 24          # hora UTC de la hora objetivo
        ang = 2 * math.pi * hora / 24
        return {"l0": l0, "d1": l0 - y(1), "d3": l0 - y(3), "d24": l0 - y(24), "m24": m24 - l0,
                "pm10d": hist[ts][1] - l0, "hs": math.sin(ang), "hc": math.cos(ang)}
    except KeyError:
        return None


def filas_entrenamiento(serie):
    """serie [(ts,pm25,pm10)] -> filas (ts, features..., objetivo=delta a +1h)."""
    hist = {t: (a, b) for t, a, b in serie}
    filas = []
    for t, a, _ in serie:
        f = features(hist, t)
        nxt = hist.get(t + HORA_MS)
        if f is None or nxt is None:
            continue
        filas.append({"ts": t, **f, "delta": nxt[0] - a})
    return filas
