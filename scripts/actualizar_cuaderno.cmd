@echo off
REM Reemplaza en el contenedor (Jupyter) el cuaderno 12_ecodata_u2.ipynb por la version limpia del repositorio.
cd /d "%~dp0.."
for /f "delims=" %%P in ('docker exec lambda26-pyspark find / -path /proc -prune -o -name 12_ecodata_u2.ipynb -print') do (
  echo Reemplazando %%P
  docker cp s12-ecodata\12_ecodata_u2.ipynb lambda26-pyspark:%%P
)
docker exec lambda26-pyspark ls -la /tmp/s12/eval_anios.json /tmp/s12/latencia_demo.py
echo LISTO. Cierra la pestana del cuaderno en Jupyter sin guardar y vuelve a abrirlo.
