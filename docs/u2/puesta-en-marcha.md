# Puesta en marcha y demo

## Requisitos
| Requisito | Versión usada | Nota |
|---|---|---|
| Docker Desktop (Compose v2) | reciente | WSL2 activado en Windows |
| Contenedor `lambda26-pyspark` | Spark 4.2.0 | Jupyter en <http://localhost:4488> |
| Git y navegador | cualquiera | Para clonar y abrir Grafana, Prometheus y Kafka UI |

Puertos del host (solo `127.0.0.1`): Kafka 49092 · Prometheus 49090 · Pushgateway 49091 · Grafana 43000 · Kafka UI 48085 · exporter 49308 · Jupyter 4488.

## Arranque en un solo comando (Windows, CMD)
```bat
cd /d D:\ACUÑA\lambda26-bigdata
scripts\levantar.cmd
```
`levantar.cmd` levanta la red, Kafka (`up -d --wait`, espera a *healthy*), la observabilidad, el contenedor de Spark (copia `s12-aire/` a `/tmp/s12`), crea los topics y ejecuta `probar_salud.cmd`. Termina en `LISTO.`

## Tableros de Grafana
Dashboards → Import → subir `s12-aire/grafana/dashboard_s12_aire.json` (tablero U2, 16 paneles) y `u3-integracion/grafana/dashboard_u3_integracion.json` (tablero de integración). Fuente: *Prometheus*.

## Demo continuo (tableros siempre con datos)
1. `scripts\demo_continuo.cmd` crea `atmos-aire-demo`, arranca el job de streaming dentro del contenedor y publica **1 evento cada 10 s** desde 2023-10-20 (el periodo de prueba de U1), y luego la API de Open-Meteo cada hora.
2. Las métricas de error (MAE) aparecen cuando el modelo reúne 24 h de historia.
3. **Reinicio limpio:** `scripts\reiniciar_demo.cmd [segundos_por_evento]`.
4. **Detener:** `scripts\detener_demo.cmd`.

## Guion de demo (5 min)
| Min | Acción | Qué se ve |
|---|---|---|
| 0:00 | `docker ps` | Servicios con healthcheck en `(healthy)` |
| 0:30 | Grafana, tablero S12 Calidad del aire | PM2.5 real vs predicción, MAE modelo vs persistencia |
| 1:30 | Kafka UI | Topics y mensajes de `atmos-aire`, histórico 2024 y 2025 |
| 2:30 | Prometheus → Targets y Alerts | `pushgateway`, `kafka-exporter` y `prometheus` en UP; 5 reglas |
| 3:15 | Publicar con `--tardio 30` | Sube «Filas descartadas por watermark» y se dispara la alerta informativa |
| 4:00 | Cuaderno de integración | 19/19 compuertas y manifiesto |
| 4:45 | Cierre | Resultados y mejoras |

## Si algo falla
| Falla | Qué hacer |
|---|---|
| Docker no levanta o va lento | Mostrar las capturas de [Evidencias](evidencias.md) y el último manifiesto |
| Kafka no responde | Ejecutar el cuaderno de integración con `FUENTE=archivos`, que lee el JSON local |
| Prometheus no confirma en 90 s | Consultar `lambda26_aire_mae_modelo` a mano en <http://localhost:49090/graph>; el scrape es cada 15 s |
| Grafana vacío | Recargar con *Last 3 hours*, o mostrar la salida del cuaderno |
| Sin Internet | La demo no depende de Internet: usa el JSON local |

## Si algo falla al arrancar
| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| `levantar` espera en Kafka | El broker aún arranca | `docker logs lambda26-kafka --tail 50` |
| Falla «Prometheus scrapea Pushgateway» | Falta el job en `prometheus.yml` | Verificar <http://localhost:49090/targets> |
| `container name ... is already in use` | Quedó un contenedor creado a mano | `docker rm -f lambda26-pushgateway` y repetir |
