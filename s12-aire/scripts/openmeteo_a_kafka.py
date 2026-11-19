"""Publica PM2.5/PM10 de Open-Meteo (Air Quality, modelo CAMS) en el topic `atmos-aire`.
Dato MODELADO en grilla (~45 km), horario: no es una estación. timestamp = hora del dato (tiempo de evento).

Modos:
  --archivo aq_juliaca_2023.json   replay de un JSON ya descargado (--desde/--hasta, --pausa seg entre eventos)
  --api                            consulta la API y publica las horas ya ocurridas (past_days=2)
  --tardio HORAS                   (demo) publica 1 evento con event-time = última hora - HORAS, origen demo-tardio
  --dry-run                        imprime los JSON en vez de publicar
"""
import argparse, json, sys, time, urllib.request
from aire_comun import leer_json, evento, a_ms, HORA_MS, LAT, LON

API = ("https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lon}"
       "&hourly=pm10,pm2_5&past_days=2&forecast_days=1&timezone=GMT")


def desde_api():
    with urllib.request.urlopen(API.format(lat=LAT, lon=LON), timeout=30) as r:
        h = json.load(r)["hourly"]
    ahora = int(time.time() * 1000)
    return sorted((a_ms(t), a, b) for t, a, b in zip(h["time"], h["pm2_5"], h["pm10"])
                  if a is not None and b is not None and a_ms(t) <= ahora)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--archivo"); p.add_argument("--api", action="store_true")
    p.add_argument("--desde", help="AAAA-MM-DDTHH:MM"); p.add_argument("--hasta")
    p.add_argument("--pausa", type=float, default=0.0)
    p.add_argument("--tardio", type=int, metavar="HORAS")
    p.add_argument("--bootstrap", default="kafka:9092"); p.add_argument("--topic", default="atmos-aire")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    if not (a.archivo or a.api):
        p.error("indica --archivo o --api")
    serie = leer_json(a.archivo) if a.archivo else desde_api()
    origen = "openmeteo-replay" if a.archivo else "openmeteo-cams"
    if a.desde: serie = [s for s in serie if s[0] >= a_ms(a.desde)]
    if a.hasta: serie = [s for s in serie if s[0] <= a_ms(a.hasta)]
    if not serie: sys.exit("sin datos en el rango")
    if a.tardio is not None:
        t, pm25, pm10 = serie[-1]
        eventos = [evento(t - a.tardio * HORA_MS, pm25, pm10, "demo-tardio")]
    else:
        eventos = [evento(t, x, y, origen) for t, x, y in serie]
    prod = None
    if not a.dry_run:
        from kafka import KafkaProducer
        prod = KafkaProducer(bootstrap_servers=a.bootstrap, acks="all", linger_ms=20,
                             key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode())
    for i, e in enumerate(eventos, 1):
        if prod: prod.send(a.topic, key=e["sensorId"], value=e)
        else: print(json.dumps(e))
        if a.pausa: time.sleep(a.pausa)
    if prod:
        prod.flush(); print(f"{len(eventos)} eventos -> {a.topic} ({eventos[0]['origen']})")


if __name__ == "__main__":
    main()
