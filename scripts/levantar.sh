#!/usr/bin/env bash
# Equivalente a levantar.cmd para Linux/macOS/WSL.
set -euo pipefail
cd "$(dirname "$0")/.."
ENVF=(); [ -f .env ] && ENVF=(--env-file .env)
docker network inspect lambda26-kafka-net >/dev/null 2>&1 || docker network create lambda26-kafka-net
docker compose "${ENVF[@]}" -f kafka/compose.yml up -d --wait
if ! docker ps -aq -f name='^lambda26-pushgateway$' -f label=com.docker.compose.project=lambda26-obs | grep -q .; then
  docker rm -f lambda26-pushgateway >/dev/null 2>&1 || true
fi
docker compose "${ENVF[@]}" -f obs/compose.yml up -d --wait
docker start lambda26-pyspark >/dev/null
docker exec lambda26-pyspark mkdir -p /tmp/s12
docker cp s12-aire/scripts lambda26-pyspark:/tmp/s12/
docker cp s12-aire/modelo_pm25 lambda26-pyspark:/tmp/s12/
docker cp s12-aire/aq_juliaca_2023.json lambda26-pyspark:/tmp/s12/
for t in atmos-aire atmos-negocio; do
  docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done
echo "LISTO"
