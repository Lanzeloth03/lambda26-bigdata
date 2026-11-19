# Hardening: lista de comprobación

Revisión técnica final de la infraestructura (`kafka/compose.yml`, `obs/compose.yml`, scripts). Estado **antes → después** de esta revisión. ✔ = aplicado y verificable en el repositorio; ⚠ = riesgo aceptado (ver [riesgos](noregresion-riesgos.md)).

| # | Control | Antes | Después | Evidencia |
|---|---|---|---|---|
| 1 | Imágenes con versión fija (nunca `latest`) | ✔ kafka 4.3.1, exporter v1.10.0, ui v1.5.0, prometheus v3.14.0, grafana 11.4.0 | ✔ + pushgateway **v1.11.0** | `grep image: */compose.yml` |
| 2 | Política de reinicio | ✔ `unless-stopped` | ✔ en los 6 servicios | compose |
| 3 | *Healthchecks* | **0 de 5** servicios | ✔ **5 de 6** (kafka, kafka-ui, prometheus, pushgateway, grafana) | `docker ps` → `(healthy)` |
| 4 | Espera por dependencias | `depends_on` sin condición (arrancaba sin esperar) | ✔ `condition: service_healthy` (exporter y UI esperan a Kafka; Grafana a Prometheus) | compose; `up -d --wait` |
| 5 | Credenciales fuera del código | `admin/admin` escritas en el compose | ✔ `${GRAFANA_ADMIN_USER:-admin}` / `${GRAFANA_ADMIN_PASSWORD:-admin}`; `.env` ignorado; `.env.example` | `git diff`, `.gitignore` |
| 6 | Pushgateway reproducible | contenedor creado a mano, ausente del compose | ✔ servicio en `obs/compose.yml`; `levantar` retira el antiguo | H3 en [bitácora](bitacora.md) |
| 7 | Superficie de red | todos los puertos publicados en `0.0.0.0` | ✔ solo `127.0.0.1:<puerto>`; la comunicación interna usa la red `lambda26-kafka-net` | compose |
| 8 | Configuración versionada | `prometheus.yml` y `alerts.yml` sin commit | ✔ versionados | `git log` |
| 9 | Aislamiento de métricas de prueba | las pruebas pisaban el job del tablero | ✔ `--job lambda26_aire_int` | H6 |
| 10 | Secretos en el repositorio | ninguno hallado | ✔ `.env`, `secrets.h` en `.gitignore` | `git grep -i password` |
| 11 | Autenticación de Kafka / Pushgateway | ninguna | ⚠ aceptado (laboratorio local) | R2, R3 |
| 12 | Persistencia de Kafka y Pushgateway | sin volumen | ⚠ aceptado (datos reproducibles por replay) | R3, R7 |
| 13 | Límites de CPU/memoria | ninguno | ⚠ aceptado | R8 |
| 14 | Escaneo de vulnerabilidades de imágenes | no hecho | ⚠ aceptado | R9 |

## Cómo verificar en vivo (durante la sustentación)
```bat
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
scripts\probar_salud.cmd
docker compose -f obs/compose.yml config | findstr /i "image healthcheck condition 127.0.0.1"
```
Todo debe mostrar `(healthy)`, puertos en `127.0.0.1` y versiones fijas.
