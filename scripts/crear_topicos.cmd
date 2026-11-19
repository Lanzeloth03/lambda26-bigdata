@echo off
rem Crea los topicos del proyecto Aire (idempotente: --if-not-exists).
for %%t in (atmos-aire atmos-negocio) do (
  docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic %%t --partitions 1 --replication-factor 1
)
