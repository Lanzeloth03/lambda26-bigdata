"""Genera docs/evidencias/corridas.md a partir de u3-integracion/manifiestos/manifest_*.json.
Uso (desde la raíz del repo):  python scripts/resumen_manifiestos.py
"""
import glob, json, os, sys

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILES = sorted(glob.glob(os.path.join(RAIZ, "u3-integracion", "manifiestos", "manifest_*.json")))
OUT = os.path.join(RAIZ, "docs", "evidencias", "corridas.md")

def f(x, n=4): return f"{x:.{n}f}"

L = ["# Corridas de integración", "",
     "Página **generada** por `python scripts/resumen_manifiestos.py` desde `u3-integracion/manifiestos/`. No se edita a mano.", ""]
if not FILES:
    sys.exit("No hay manifiestos en u3-integracion/manifiestos/. Trae los del contenedor: "
             "docker cp lambda26-pyspark:/tmp/s12/manifiestos/. u3-integracion/manifiestos/")
ms = [json.load(open(p, encoding="utf-8")) for p in FILES]
L += ["| runId (UTC) | fuente | horas eval. | MAE modelo | MAE persist. | mejora % | err p50 | err p95 | compuertas OK | duración (s) |", "|---|---|---|---|---|---|---|---|---|---|"]
for m in ms:
    s, r = m["streaming"], m["resumen"]
    L.append(f'| `{m["runId"]}` | {m["fuente"]} | {s["n_evaluadas"]} | {f(s["mae_modelo"],6)} | {f(s["mae_persistencia"],6)} | {f(s["mejora_vs_persistencia_pct"],2)} | '
             f'{f(s["err_abs_modelo_p50"])} | {f(s["err_abs_modelo_p95"])} | {r["gates_ok"]}/{r["gates_total"]} | {m["duracionTotalS"]} |')
L.append("")
if len(ms) >= 2:
    a, b = ms[-2]["streaming"], ms[-1]["streaming"]
    ok = a["n_evaluadas"] == b["n_evaluadas"] and abs(a["mae_modelo"] - b["mae_modelo"]) < 1e-9 and abs(a["err_abs_modelo_p95"] - b["err_abs_modelo_p95"]) < 1e-9
    L.append(f'**Comparación de las dos últimas corridas:** {"idénticas en métrica (sin regresión)" if ok else "DIFERENTES: investigar regresión"}. '
             f'Δ duración = {ms[-1]["duracionTotalS"] - ms[-2]["duracionTotalS"]:+.1f} s.')
else:
    L.append("**Solo hay una corrida:** el criterio pide al menos dos.")
L += ["", "## Compuertas de la última corrida", "", "| compuerta | fase | estado | detalle |", "|---|---|---|---|"]
for g in ms[-1]["gates"]:
    L.append(f'| {g["gate"]} | {g["fase"]} | {g["estado"]} | {str(g["detalle"]).replace("|", "/")[:110]} |')
L += ["", "## Fallas demostradas a propósito", ""]
for d in ms[-1].get("gatesDemostracion", []):
    L.append(f'* **{d["gate"]}** → {d["resultado"]}: {d["motivo"]}')
L += ["", "## Observabilidad (última corrida)", "", "```json", json.dumps(ms[-1]["observabilidad"], indent=2, ensure_ascii=False), "```", ""]
open(OUT, "w", encoding="utf-8").write("\n".join(L))
print("escrito", OUT, "con", len(ms), "corridas")
