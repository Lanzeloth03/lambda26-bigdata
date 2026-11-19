# Bitácora de la revisión técnica

Hallazgos reales, con su **prueba** y su **corrección**. Formato: hallazgo → evidencia → decisión → verificación.

| ID | Hallazgo | Evidencia | Corrección | Verificación |
|---|---|---|---|---|
| H1 | Ningún servicio tenía *healthcheck*; `depends_on` solo ordenaba el arranque sin esperar | `git show HEAD:kafka/compose.yml`: 0 bloques `healthcheck` | Healthchecks y `condition: service_healthy` | `docker ps` → `(healthy)`; ver [hardening](hardening.md) #3–4 |
| H2 | Clave de Grafana `admin/admin` escrita en el compose versionado | `git show HEAD:obs/compose.yml` | Variables `GRAFANA_ADMIN_*` + `.env.example` | `git grep -n "ADMIN_PASSWORD" obs/` solo muestra `${…}` |
| H3 | El Pushgateway **no estaba en ningún compose**: se creaba a mano y el runbook decía `docker start lambda26-pushgateway`; un equipo nuevo no podía reproducir el sistema | runbook de arranque vs `obs/compose.yml` | Servicio `pushgateway` (v1.11.0) con healthcheck y alias de red; `levantar` elimina el contenedor manual | `levantar.cmd` termina sin error de nombre en uso |
| H4 | `prometheus.yml` (job `pushgateway`) y `alerts.yml` existían solo en el equipo, sin commit | `git status`: modificados/sin seguimiento | Se versionan | `git log -- obs/prometheus` |
| H5 | Todos los puertos publicados en todas las interfaces | `ports: "49090:9090"` | Se publican en `127.0.0.1` | `docker ps` muestra `127.0.0.1:…` |
| H6 | El job de Spark empujaba siempre a `lambda26_aire`: una prueba de integración habría pisado las métricas del tablero en vivo | `JOB = "lambda26_aire"` fijo | Argumento `--job` (valor por defecto sin cambio) | corrida con `lambda26_aire_int`; el tablero U2 intacto |
| H7 | **Mi propia compuerta S3 estaba mal calibrada:** exigía ≥ 150 horas evaluadas y bloqueó una corrida correcta de 143 (168 − 24 h de historia − 1 sin hora siguiente) | primera ejecución de prueba: `CompuertaBloqueada: S3 … 143 horas evaluadas` | Criterio exacto `== 143` con su justificación en el nombre de la compuerta | 2 corridas completas, 19/19 OK |
| H8 | El modelo guardado con Spark 4.2.0 **no se puede cargar** con Spark 4.0.1 (`treeID cannot be resolved`) | error al cargar el modelo en un entorno de prueba con Spark 4.0.1 | Se documenta la versión de Spark requerida (4.2.0) en [Instalación](ejecucion.md); previa P5 verifica que el modelo exista | el contenedor oficial usa 4.2.0 |
| H9 | La corrida de integración **no ejercita el watermark**: no publica datos tardíos, así que `descartadas_watermark = 0` por diseño; si se creyera que lo prueba, el 0 daría falsa confianza | manifiesto: `descartadas_watermark: 0` | La prueba del watermark queda en la demo en vivo (`--api --tardio 30`) y en la alerta `DescartesPorWatermarkCreciendo` | demo en vivo: sube `filas_descartadas_watermark_total` |

## Lecciones
* Una compuerta mal calibrada también es un defecto: se prueba con datos buenos **y** malos (por eso el cuaderno ejecuta dos fallas a propósito).
* Lo que no está en el compose no existe para el siguiente equipo (H3).
