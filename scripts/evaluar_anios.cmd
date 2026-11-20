@echo off
REM Evalua el modelo (entrenado con 2023) sobre 2024, 2025 y 2026 (Open-Meteo real) y trae el resultado a s12-aire\eval_anios.json
REM Uso: scripts\evaluar_anios.cmd      (necesita internet dentro del contenedor)
cd /d "%~dp0.."
docker cp s12-aire\scripts\evaluar_anios.py lambda26-pyspark:/tmp/s12/scripts/evaluar_anios.py
docker exec lambda26-pyspark python /tmp/s12/scripts/evaluar_anios.py 2024 2025 2026
docker cp lambda26-pyspark:/tmp/s12/eval_anios.json s12-aire\eval_anios.json
echo LISTO. Resultado en s12-aire\eval_anios.json
