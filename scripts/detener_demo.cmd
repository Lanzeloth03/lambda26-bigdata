@echo off
REM Detiene el job y los alimentadores del demo continuo (el contenedor no trae pkill; se usa detener_todo.py).
cd /d "%~dp0.."
docker cp scripts\detener_todo.py lambda26-pyspark:/tmp/s12/detener_todo.py
docker exec lambda26-pyspark python /tmp/s12/detener_todo.py
