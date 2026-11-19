@echo off
rem Previa de salud: cada servicio responde donde debe. Devuelve 1 si alguno falla.
set "FALLOS=0"
call :chk Kafka-UI      http://localhost:48085/actuator/health
call :chk Prometheus    http://localhost:49090/-/healthy
call :chk Pushgateway   http://localhost:49091/-/healthy
call :chk Grafana       http://localhost:43000/api/health
call :chk Jupyter       http://localhost:4488/api
for /f %%s in ('docker inspect -f "{{.State.Health.Status}}" lambda26-kafka 2^>nul') do echo   Kafka healthcheck: %%s
exit /b %FALLOS%
:chk
curl -s -o nul -f %2 && (echo   OK    %1) || (echo   FALLA %1 ^(%2^) & set /a FALLOS+=1)
exit /b 0
