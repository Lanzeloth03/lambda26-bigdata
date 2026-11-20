"""Mata TODOS los procesos del demo dentro del contenedor (alimentadores, bucles y job de streaming).
El contenedor no trae ps/pkill, asi que se recorre /proc. Se ejecuta con: python /tmp/s12/detener_todo.py"""
import os, signal
CLAVES = ("openmeteo_a_kafka", "s12_streaming_aire")
yo = os.getpid(); n = 0
for p in os.listdir("/proc"):
    if not p.isdigit() or int(p) == yo: continue
    try: cmd = open(f"/proc/{p}/cmdline", "rb").read().decode(errors="ignore")
    except OSError: continue
    if any(k in cmd for k in CLAVES) and "detener_todo" not in cmd:
        try: os.kill(int(p), signal.SIGKILL); n += 1
        except OSError: pass
for f in ("job.pid", "alim.pid"):
    try: os.remove(f"/tmp/s12/{f}")
    except OSError: pass
print(f"procesos detenidos: {n}")
