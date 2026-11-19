# Corridas de integración

Esta página se **genera** con `python scripts/resumen_manifiestos.py` a partir de los `manifest_<runId>.json` que produce el cuaderno 13. No se edita a mano.

**Aún no hay corridas registradas en el repositorio.**

Cómo registrar las del laboratorio:

1. Ejecuta `13_integracion_dataops.ipynb` dos veces (*Restart & Run All*).
2. Trae los manifiestos: `docker cp lambda26-pyspark:/tmp/s12/manifiestos/. u3-integracion/manifiestos/`
3. `python scripts/resumen_manifiestos.py` → reescribe esta página.
4. Commit: `git add u3-integracion/manifiestos docs/evidencias/corridas.md`.
