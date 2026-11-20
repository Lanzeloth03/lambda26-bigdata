# 2. Instalación y ejecución

## Requisitos
| Requisito | Versión usada | Nota |
|---|---|---|
| Docker Desktop (Compose v2) | reciente | WSL2 activado en Windows |
| Contenedor `lambda26-pyspark` | Spark **4.2.0** | Jupyter en <http://localhost:4488>. El modelo guardado con Spark 4.2 **no carga en Spark 4.0** (ver [bitácora](bitacora.md), H8). |
| Git | cualquiera | para clonar |
| Navegador | cualquiera | Grafana, Prometheus, Kafka UI |

Puertos usados en el host (solo `127.0.0.1`): Kafka 49092 · Prometheus 49090 · Pushgateway 49091 · Grafana 43000 · Kafka UI 48085 · exporter 49308 · Jupyter 4488.

## Configuración (credenciales fuera del código)
```bat
copy .env.example .env
notepad .env          :: cambiar GRAFANA_ADMIN_PASSWORD
```
`.env` está en `.gitignore`. Si no existe, Grafana usa `admin/admin` (solo laboratorio local; ver riesgos aceptados).

## Arranque en un solo comando (Windows, CMD)
```bat
cd /d D:\ACUÑA\lambda26-bigdata
scripts\levantar.cmd
```
`levantar.cmd` hace, en orden: red → Kafka (`up -d --wait`, espera *healthy*) → retira el Pushgateway antiguo creado a mano → observabilidad (`up -d --wait`) → inicia `lambda26-pyspark` y copia `s12-aire/` a `/tmp/s12` → crea los topics → `probar_salud.cmd`. Equivalente Linux/WSL: `scripts/levantar.sh`.

Salida esperada del final:
```
  OK    Kafka-UI   OK    Prometheus   OK    Pushgateway   OK    Grafana   OK    Jupyter
  Kafka healthcheck: healthy
LISTO.
```

## Arranque manual (si prefieres ver cada paso)
```bat
docker compose --env-file .env -f kafka/compose.yml up -d --wait
docker compose --env-file .env -f obs/compose.yml up -d --wait
docker start lambda26-pyspark
docker exec lambda26-pyspark mkdir -p /tmp/s12
docker cp s12-aire\scripts lambda26-pyspark:/tmp/s12/
docker cp s12-aire\modelo_pm25 lambda26-pyspark:/tmp/s12/
docker cp s12-aire\aq_juliaca_2023.json lambda26-pyspark:/tmp/s12/
scripts\crear_topicos.cmd
scripts\probar_salud.cmd
```

## Importar el tablero
Grafana → Dashboards → Import → subir `s12-aire/grafana/dashboard_s12_aire.json` (tablero U2) y `u3-integracion/grafana/dashboard_u3_integracion.json` (tablero U3); elegir la fuente *Prometheus*.

## Apagar
```bat
docker compose -f obs/compose.yml down
docker compose -f kafka/compose.yml down
```
(los volúmenes de Prometheus y Grafana se conservan; añade `-v` solo si quieres borrarlos).

## Si algo falla
| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| `levantar` se queda esperando en Kafka | Docker aún sin recursos / broker arrancando | `docker logs lambda26-kafka --tail 50`; el `start_period` es 20 s y reintenta 12 veces |
| `P4 Prometheus scrapea Pushgateway` falla | falta el job en `prometheus.yml` | verificar <http://localhost:49090/targets> (debe haber `pushgateway` UP) |
| `container name lambda26-pushgateway is already in use` | quedó el contenedor creado a mano | `docker rm -f lambda26-pushgateway` y repetir (el script ya lo hace) |
| Grafana no abre con la clave nueva | el volumen conserva la clave antigua | `docker compose -f obs/compose.yml down -v` y volver a levantar (o cambiarla desde la UI) |

## Demo continuo (tableros siempre con datos)
Después de `scripts\levantar.cmd`, ejecuta `scripts\demo_continuo.cmd`. Crea el topic `atmos-aire-demo`, arranca el job de streaming dentro del contenedor y publica **1 evento cada 10 s** desde 2023-10-20 (el periodo de test de U1, unas 4,9 h de replay) y luego la API de Open-Meteo cada hora. No necesita una terminal abierta. Las métricas de error (MAE) aparecen cuando el modelo reúne 24 h de historia.

- **Reiniciar limpio** (Grafana vuelve a cero, sin duplicados): `scripts\reiniciar_demo.cmd [segundos_por_evento]`.
- **Detener**: `scripts\detener_demo.cmd`. El contenedor no trae `ps` ni `pkill`, por eso se usa `scripts\detener_todo.py`, que recorre `/proc`.
- **Volumen en Kafka**: `scripts\cargar_historico.cmd` carga el año 2023 completo en `atmos-aire` (8 760 mensajes) y `scripts\cargar_historico_anio.cmd 2024` descarga de Open-Meteo y publica otro año real en `atmos-aire-hist-2024`. Son topics de ingesta (Bronze en Kafka): el job del demo solo lee `atmos-aire-demo`.
- El tablero incluye el panel «Mensajes por topic», con la métrica del `kafka-exporter`.
