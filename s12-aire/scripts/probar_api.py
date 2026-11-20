"""Prueba la conexion real a Open-Meteo desde el contenedor y (opcional) publica las ultimas horas en atmos-aire.
Uso: python probar_api.py            -> solo prueba DNS + API (dry-run, no publica)
     python probar_api.py --publicar -> ademas publica las ultimas 3 h reales (origen=openmeteo-cams) en atmos-aire
"""
import socket, subprocess, sys, datetime as dt
H = "air-quality-api.open-meteo.com"
print("UTC ahora:", dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
try:
    print("DNS", H, "->", socket.gethostbyname(H))
except Exception as e:
    print("DNS FALLA:", e); sys.exit(1)
desde = (dt.datetime.utcnow() - dt.timedelta(hours=3)).strftime("%Y-%m-%dT%H:00")
base = ["python", "/tmp/s12/scripts/openmeteo_a_kafka.py", "--api", "--desde", desde]
if "--publicar" in sys.argv:
    r = subprocess.run(base + ["--topic", "atmos-aire"], capture_output=True, text=True)
else:
    r = subprocess.run(base + ["--dry-run"], capture_output=True, text=True)
print(r.stdout[-1500:]); print(r.stderr[-800:])
