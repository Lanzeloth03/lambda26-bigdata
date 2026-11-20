@echo off
REM Detiene el job y el alimentador del demo continuo.
docker exec lambda26-pyspark sh -c "kill $(cat /tmp/s12/alim.pid) 2>/dev/null; pkill -f openmeteo_a_kafka 2>/dev/null; kill $(cat /tmp/s12/job.pid) 2>/dev/null; pkill -f s12_streaming_aire 2>/dev/null; rm -f /tmp/s12/*.pid; echo detenido"
