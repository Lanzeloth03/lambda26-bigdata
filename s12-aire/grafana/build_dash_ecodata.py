"""Tablero unificado EcoData U2: aire (PM2.5) + clima (temperatura) en una sola pantalla. Genera dashboard_s12_ecodata.json"""
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
        d["options"] = {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"}
    return d


def fila(title, y):
    pid[0] += 1
    return {"id": pid[0], "type": "row", "title": title, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def seccion(pre, job, y0, nombre, var, unid, ent, umbral, ev, ev_titulo, modelo, serie, vent):
    M = f"lambda26_{pre}_"
    return [
        fila(nombre, y0),
        panel("stat", f"{var} última hora ({unid})", [M + ent + "_ultim" + ("o" if pre == "aire" else "a")], 0, y0 + 1, dec=1),
        panel("stat", f"Predicción {var} próxima hora", [M + f"pred_{ent}_proxima_hora"], 6, y0 + 1, dec=1),
        panel("stat", f"MAE modelo ({unid})", [M + "mae_modelo"], 12, y0 + 1, dec=3),
        panel("stat", "MAE persistencia (baseline)", [M + "mae_persistencia"], 18, y0 + 1, dec=3),
        panel("stat", "Mejora vs persistencia", [M + "mejora_pct"], 0, y0 + 5, unit="percent", dec=1),
        panel("stat", "Horas evaluadas", [M + "n_evaluadas"], 6, y0 + 5, dec=0),
        panel("stat", ev_titulo, [M + ev], 12, y0 + 5, dec=0),
        panel("stat", "Filas descartadas por watermark", [M + "filas_descartadas_watermark_total"], 18, y0 + 5, dec=0),
        panel("timeseries", f"{var}: real vs predicción de la hora siguiente",
              [M + ent + "_ultim" + ("o" if pre == "aire" else "a"), M + f"pred_{ent}_proxima_hora", M + umbral], 0, y0 + 9, w=12, h=8,
              unit="none", legends=["real (última hora)", "predicción +1 h", "umbral"]),
        panel("timeseries", "Error: modelo vs persistencia (MAE acumulado)", [M + "mae_modelo", M + "mae_persistencia"], 12, y0 + 9, w=12, h=8,
              legends=[modelo, "persistencia"]),
        panel("timeseries", f"Ventana de 6 h: {var} " + vent[0], [M + v for v in vent[1]], 0, y0 + 17, w=12, h=7, legends=vent[2]),
        panel("timeseries", "Estado y descartes del watermark", [M + "estado_filas", M + "filas_descartadas_watermark_total"], 12, y0 + 17, w=12, h=7,
              legends=["filas de estado", "descartadas (acum.)"]),
        panel("stat", "Segundos desde el último push (inferencia)", [f'time() - push_time_seconds{{job="lambda26_{job}",instance="inferencia"}}'],
              0, y0 + 24, unit="s", dec=0),
        panel("timeseries", "Eventos procesados", [M + "eventos_total"], 6, y0 + 24, w=18, h=5, legends=["eventos"]),
    ]


ps = [fila("Resumen de alerta temprana EcoData (aire + clima)", 0),
      panel("stat", "PM2.5 próxima hora (µg/m³)", ["lambda26_aire_pred_pm25_proxima_hora"], 0, 1, dec=1),
      panel("stat", "Temperatura próxima hora (°C)", ["lambda26_clima_pred_temp_proxima_hora"], 6, 1, dec=1),
      panel("stat", "Episodios de aire (> p95)", ["lambda26_aire_episodios_total"], 12, 1, dec=0),
      panel("stat", "Heladas (< umbral)", ["lambda26_clima_heladas_total"], 18, 1, dec=0)]
ps += seccion("aire", "aire", 5, "Dimensión Aire (Lanzeloth): PM2.5 a +1 h", "PM2.5", "µg/m³", "pm25", "umbral_p95",
              "episodios_total", "Episodios (> p95 de entrenamiento)", "modelo GBT", None,
              ("promedio y máximo", ["ventana_pm25_prom", "ventana_pm25_max"], ["promedio", "máximo"]))
ps += seccion("clima", "clima", 35, "Dimensión Clima (Alahin): temperatura a +1 h", "Temperatura", "°C", "temp", "umbral_helada",
              "heladas_total", "Heladas (< umbral)", "modelo GBT", None,
              ("promedio y mínimo", ["ventana_temp_prom", "ventana_temp_min"], ["promedio", "mínimo"]))
dash = {"uid": "lambda26-s12-ecodata", "title": "S12 - EcoData U2: aire + clima (Open-Meteo -> Kafka -> Spark)", "schemaVersion": 39, "version": 1,
        "refresh": "5s", "time": {"from": "now-1h", "to": "now"}, "tags": ["lambda26", "u2", "ecodata"], "panels": ps,
        "templating": {"list": [{"name": "DS_PROMETHEUS", "type": "datasource", "query": "prometheus", "current": {}}]},
        "annotations": {"list": []}}
json.dump(dash, open("dashboard_s12_ecodata.json", "w"), indent=1, ensure_ascii=False)
print(len(ps), "paneles")
