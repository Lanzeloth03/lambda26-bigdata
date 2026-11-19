@echo off
rem Levanta EcoData Aire en orden, esperando a que cada servicio este sano (healthchecks).
rem Uso:  scripts\levantar.cmd      (desde la raiz del repo o desde cualquier carpeta)
setlocal
cd /d "%~dp0.."
if exist .env (set "ENVF=--env-file .env") else (set "ENVF=")

echo [1/5] Red y Kafka...
docker network inspect lambda26-kafka-net >nul 2>&1 || docker network create lambda26-kafka-net
docker compose %ENVF% -f kafka/compose.yml up -d --wait || goto :fallo

echo [2/5] Pushgateway antiguo (creado a mano) fuera, el de compose manda...
for /f %%i in ('docker ps -aq -f "name=^lambda26-pushgateway$" -f "label=com.docker.compose.project=lambda26-obs"') do set "OK_COMPOSE=%%i"
if not defined OK_COMPOSE docker rm -f lambda26-pushgateway >nul 2>&1

echo [3/5] Observabilidad (Prometheus, Pushgateway, Grafana)...
docker compose %ENVF% -f obs/compose.yml up -d --wait || goto :fallo

echo [4/5] Contenedor PySpark + activos de s12-aire en /tmp/s12...
docker start lambda26-pyspark >nul
docker exec lambda26-pyspark mkdir -p /tmp/s12
docker cp s12-aire/scripts lambda26-pyspark:/tmp/s12/
docker cp s12-aire/modelo_pm25 lambda26-pyspark:/tmp/s12/
docker cp s12-aire/aq_juliaca_2023.json lambda26-pyspark:/tmp/s12/

echo [5/5] Topicos y salud...
call scripts\crear_topicos.cmd
call scripts\probar_salud.cmd || goto :fallo
echo.
echo LISTO. Ejecuta el cuaderno 13_integracion_dataops.ipynb (ver docs/ejecucion.md)
exit /b 0
:fallo
echo.
echo FALLO el arranque. Revisa: docker compose -f kafka/compose.yml ps  /  docker compose -f obs/compose.yml ps
exit /b 1
