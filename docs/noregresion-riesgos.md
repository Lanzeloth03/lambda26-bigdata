# No regresión y riesgos aceptados

## Prueba de no regresión tras cada cambio
Regla: **después de cada cambio de hardening se repite la misma batería** (`probar_salud.cmd` + cuaderno 13 con 19 compuertas) y la métrica de negocio no debe cambiar.

| # | Cambio | Riesgo de regresión | Prueba | Dónde se probó | Resultado |
|---|---|---|---|---|---|
| C1 | Healthchecks en kafka, ui, prometheus, pushgateway, grafana | un `test` mal escrito deja el servicio *unhealthy* y bloquea `--wait` | `up -d --wait` y `docker ps` | YAML validado fuera de Docker; **ejecución en el laboratorio** | ver corridas |
| C2 | `depends_on: service_healthy` | arranque más lento o bucle de espera | tiempo de `levantar.cmd` | laboratorio | ver corridas |
| C3 | Grafana por variables | clave nueva no coincide con el volumen antiguo | login en <http://localhost:43000> | laboratorio | ver [Si algo falla](ejecucion.md) |
| C4 | Pushgateway como servicio | choque de nombre con el contenedor manual | `levantar.cmd` retira el antiguo | laboratorio | ver corridas |
| C5 | Puertos a `127.0.0.1` | algo accedía por la IP LAN | `probar_salud.cmd` (todo por `localhost`) | laboratorio | ver corridas |
| C6 | `--job` en `s12_streaming_aire.py` | cambiar el job por defecto rompería el tablero U2 | valor por defecto = `lambda26_aire`; corrida con `--job lambda26_aire_int` | **sandbox sin Docker** | ✔ job por defecto sin cambio; corrida con job propio OK (19/19) |
| C7 | Cuaderno 13: umbral S3 | umbral mal calculado bloquea una corrida válida | corrida completa dos veces | sandbox sin Docker | ✔ tras corregir (H7): 143 horas, 2 corridas idénticas |

**Criterio de aprobación:** `probar_salud` sin fallos **y** `n_evaluadas = 143`, `mae_modelo < mae_persistencia`, y el MAE de la corrida N igual al de la N−1.

## Tabla de riesgos aceptados
| ID | Riesgo | Por qué se acepta | Qué cambia en producción |
|---|---|---|---|
| R1 | El dato es **modelado** (CAMS, ~45 km), no medido | Basta para validar arquitectura y modelo; está declarado en toda la documentación | Estaciones o sensores reales (PM2.5 por láser) calibrados; el modelo se reentrena con ellos |
| R2 | Kafka con 1 broker, `PLAINTEXT`, sin autenticación y con auto-creación de topics | Laboratorio local, puertos solo en loopback, un productor propio | 3 brokers, RF 3, TLS + SASL, `auto.create.topics.enable=false`, ACL por topic |
| R3 | Pushgateway sin autenticación ni persistencia | Las métricas se republican en cada lote; Prometheus conserva el histórico | Autenticación/proxy inverso, `--persistence.file`, o exportar con *remote write* |
| R4 | Un solo año (2023) y una sola ubicación | Alcance del curso; la deriva se vigila con la alerta `AireModeloNoSuperaPersistencia` | Reentrenamiento programado y validación por estación del año |
| R5 | Grafana con `admin/admin` por defecto si falta `.env` | Solo escucha en `127.0.0.1`; no hay datos sensibles | Secretos en un gestor (Vault/Docker secrets), SSO |
| R6 | `kafka-exporter` sin *healthcheck* | La imagen no trae shell ni `wget`; `restart: unless-stopped` lo recupera y Prometheus alerta si cae | Imagen con *probe* HTTP o chequeo desde Kubernetes |
| R7 | Kafka sin volumen persistente | Los datos se regeneran por *replay* del JSON | Volumen persistente y respaldo |
| R8 | Sin límites de CPU/memoria por servicio | Un solo equipo de laboratorio | `mem_limit`/`cpus` o *requests/limits* en el orquestador |
| R9 | Imágenes sin escaneo de vulnerabilidades | Imágenes oficiales con versión fija; sin exposición externa | Escáner (Trivy/Grype) en CI |
| R10 | Inferencia en `foreachBatch` con `collect()` en el driver | Volumen de 1 evento/hora; simple y correcto | UDF/`transform` distribuido o servicio de modelos |
