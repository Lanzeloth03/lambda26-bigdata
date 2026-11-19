import json
DS = {"type": "prometheus", "uid": "${DS_PROMETHEUS}"}
pid = [0]
def panel(kind, title, exprs, x, y, w=6, h=4, unit="none", legends=None, dec=None):
    pid[0] += 1
    t = [{"datasource": DS, "expr": e, "refId": chr(65 + i), "legendFormat": (legends or [""] * 9)[i]} for i, e in enumerate(exprs)]
    d = {"id": pid[0], "type": kind, "title": title, "datasource": DS, "targets": t,
         "gridPos": {"x": x, "y": y, "w": w, "h": h}, "fieldConfig": {"defaults": {"unit": unit}, "overrides": []}}
    if dec is not None: d["fieldConfig"]["defaults"]["decimals"] = dec
    if kind == "stat":
        steps = [{"color": "green", "value": None}]
        if "push_time" in exprs[0]: steps += [{"color": "orange", "value": 120}, {"color": "red", "value": 600}]
        d["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": steps}
        d["fieldConfig"]["defaults"]["color"] = {"mode": "thresholds"}
    if kind == "stat": d["options"] = {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"}
    return d
M = "lambda26_aire_"
ps = [
 panel("stat", "PM2.5 última hora (µg/m³)", [M + "pm25_ultimo"], 0, 0, unit="none", dec=1),
 panel("stat", "Predicción PM2.5 próxima hora", [M + "pred_pm25_proxima_hora"], 6, 0, dec=1),
 panel("stat", "MAE modelo (µg/m³)", [M + "mae_modelo"], 12, 0, dec=3),
 panel("stat", "MAE persistencia (baseline)", [M + "mae_persistencia"], 18, 0, dec=3),
 panel("stat", "Mejora vs persistencia", [M + "mejora_pct"], 0, 4, unit="percent", dec=1),
 panel("stat", "Horas evaluadas", [M + "n_evaluadas"], 6, 4, dec=0),
 panel("stat", "Episodios (> p95 de entrenamiento)", [M + "episodios_total"], 12, 4, dec=0),
 panel("stat", "Filas descartadas por watermark", [M + "filas_descartadas_watermark_total"], 18, 4, dec=0),
 panel("timeseries", "PM2.5: real vs predicción de la hora siguiente", [M + "pm25_ultimo", M + "pred_pm25_proxima_hora", M + "umbral_p95"], 0, 8, w=12, h=8,
       legends=["real (última hora)", "predicción +1 h", "umbral p95"]),
 panel("timeseries", "Error: modelo vs persistencia (MAE acumulado)", [M + "mae_modelo", M + "mae_persistencia"], 12, 8, w=12, h=8,
       legends=["modelo GBT", "persistencia"]),
 panel("timeseries", "Ventana de 6 h: PM2.5 promedio y máximo", [M + "ventana_pm25_prom", M + "ventana_pm25_max"], 0, 16, w=12, h=7, legends=["promedio", "máximo"]),
 panel("timeseries", "Estado y descartes del watermark", [M + "estado_filas", M + "filas_descartadas_watermark_total"], 12, 16, w=12, h=7, legends=["filas de estado", "descartadas (acum.)"]),
 panel("stat", "Pushgateway: segundos desde último push (inferencia)", ['time() - push_time_seconds{job="lambda26_aire",instance="inferencia"}'], 0, 23, unit="s", dec=0),
 panel("timeseries", "Eventos procesados", [M + "eventos_total"], 6, 23, w=18, h=5, legends=["eventos"]),
]
dash = {"uid": "lambda26-s12-aire", "title": "S12 - Calidad del aire (Open-Meteo / CAMS) -> Kafka -> Spark", "schemaVersion": 39, "version": 1,
        "refresh": "5s", "time": {"from": "now-1h", "to": "now"}, "tags": ["lambda26", "u2", "aire"], "panels": ps,
        "templating": {"list": [{"name": "DS_PROMETHEUS", "type": "datasource", "query": "prometheus", "current": {}}]},
        "annotations": {"list": []}}
json.dump(dash, open("dashboard_s12_aire.json", "w"), indent=1, ensure_ascii=False)
print(len(ps), "paneles")
