"""Publica temperatura, humedad y viento de Open-Meteo en el topic `atmos-clima`.
Dato horario de un modelo/reanálisis (no una estación). timestamp = hora del dato (tiempo de evento).

Modos:
  --archivo clima_juliaca_2023.json   replay de un JSON ya descargado (--desde/--hasta, --pausa seg entre eventos)
  --api                               consulta la API y publica las horas ya ocurridas (past_days=2)
  --tardio HORAS                      (demo) publica 1 evento con event-time = última hora - HORAS, origen demo-tardio
  --dry-run                           imprime los JSON en vez de publicar
"""
import argparse, json, sys, time, urllib.request
from clima_comun import leer_json, a_serie, evento, a_ms, HORA_MS, LAT, LON, VARS

API = ("https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
       "&hourly={v}&past_days=2&forecast_days=1&timezone=GMT")


def desde_api():
    with urllib.request.urlopen(API.format(lat=LAT, lon=LON, v=VARS), timeout=30) as r:
        h = json.load(r)["hourly"]
    ahora = int(time.time() * 1000)
    return [s for s in a_serie(h) if s[0] <= ahora]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--archivo"); p.add_argument("--api", action="store_true")
    p.add_argument("--desde", help="AAAA-MM-DDTHH:MM"); p.add_argument("--hasta")
    p.add_argument("--pausa", type=float, default=0.0)
    p.add_argument("--tardio", type=int, metavar="HORAS")
    p.add_argument("--bootstrap", default="kafka:9092"); p.add_argument("--topic", default="atmos-clima")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    if not (a.archivo or a.api):
        p.error("indica --archivo o --api")
    serie = leer_json(a.archivo) if a.archivo else desde_api()
    origen = "openmeteo-replay" if a.archivo else "openmeteo-api"
    if a.desde: serie = [s for s in serie if s[0] >= a_ms(a.desde)]
    if a.hasta: serie = [s for s in serie if s[0] <= a_ms(a.hasta)]
    if not serie: sys.exit("sin datos en el rango")
    if a.tardio is not None:
        t, x, y, z = serie[-1]
        eventos = [evento(t - a.tardio * HORA_MS, x, y, z, "demo-tardio")]
    else:
        eventos = [evento(t, x, y, z, origen) for t, x, y, z in serie]
    prod = None
    if not a.dry_run:
        from kafka import KafkaProducer
        prod = KafkaProducer(bootstrap_servers=a.bootstrap, acks="all", linger_ms=20,
                             key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode())
    for e in eventos:
        if prod: prod.send(a.topic, key=e["sensorId"], value=e)
        else: print(json.dumps(e))
        if a.pausa: time.sleep(a.pausa)
    if prod:
        prod.flush(); print(f"{len(eventos)} eventos -> {a.topic} ({eventos[0]['origen']})")


if __name__ == "__main__":
    main()
