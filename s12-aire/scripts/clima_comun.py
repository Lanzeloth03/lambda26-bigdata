"""Piezas compartidas entre entrenamiento y servicio del perfil CLIMA (temperatura a +1 h).
Un solo lugar define el contrato del evento y la construcción de features (sin training-serving skew)."""
import json, math
from datetime import datetime, timezone

HORA_MS = 3_600_000
ESQUEMA = ("tipoEvento STRING, sensorId STRING, temp DOUBLE, hum DOUBLE, viento DOUBLE, "
           "origen STRING, timestamp LONG")
SENSOR_ID = "openmeteo-clima-juliaca"
LAT, LON = -15.5, -70.13
FEATURES = ["l0", "d1", "d3", "d24", "m24", "hum", "humd", "viento", "hs", "hc"]
VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m"


def a_ms(iso):
    """'2023-01-01T05:00' (GMT) -> epoch ms."""
    return int(datetime.strptime(iso, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc).timestamp() * 1000)


def a_serie(h):
    """Bloque `hourly` de Open-Meteo -> lista ordenada [(ts_ms, temp, hum, viento)] sin huecos."""
    out = [(a_ms(t), a, b, c) for t, a, b, c in
           zip(h["time"], h["temperature_2m"], h["relative_humidity_2m"], h["wind_speed_10m"])
           if a is not None and b is not None and c is not None]
    return sorted(out)


def leer_json(ruta):
    return a_serie(json.load(open(ruta, encoding="utf-8"))["hourly"])


def evento(ts, temp, hum, viento, origen):
    return {"tipoEvento": "clima.lectura", "sensorId": SENSOR_ID, "temp": float(temp), "hum": float(hum),
            "viento": float(viento), "origen": origen, "timestamp": int(ts)}


def features(hist, ts):
    """hist: dict ts_ms -> (temp, hum, viento). Features de la hora `ts` para predecir ts+1h.
    Devuelve None si falta historia (se necesitan 24 h previas completas)."""
    try:
        y = lambda k: hist[ts - k * HORA_MS][0]
        l0 = y(0)
        m24 = sum(y(k) for k in range(24)) / 24
        hora = ((ts // HORA_MS) + 1) % 24           # hora UTC de la hora objetivo
        ang = 2 * math.pi * hora / 24
        return {"l0": l0, "d1": l0 - y(1), "d3": l0 - y(3), "d24": l0 - y(24), "m24": m24 - l0,
                "hum": hist[ts][1], "humd": hist[ts][1] - hist[ts - HORA_MS][1], "viento": hist[ts][2],
                "hs": math.sin(ang), "hc": math.cos(ang)}
    except KeyError:
        return None


def filas_entrenamiento(serie):
    """serie [(ts,temp,hum,viento)] -> filas (ts, features..., delta a +1h, delta_diario).
    delta_diario = cambio de la misma hora del día anterior: baseline estacional (la temperatura tiene ciclo diurno)."""
    hist = {t: (a, b, c) for t, a, b, c in serie}
    filas = []
    for t, a, _, _ in serie:
        f = features(hist, t)
        nxt, ayer0, ayer1 = hist.get(t + HORA_MS), hist.get(t - 24 * HORA_MS), hist.get(t - 23 * HORA_MS)
        if f is None or nxt is None or ayer1 is None:
            continue
        filas.append({"ts": t, **f, "delta": nxt[0] - a, "delta_diario": ayer1[0] - ayer0[0]})
    return filas
