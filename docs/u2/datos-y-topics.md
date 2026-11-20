# Datos y topics en Kafka

## Fuente
**Open-Meteo Air Quality API** (modelo **CAMS**), punto de Juliaca (lat −15,5 · lon −70,13), variables `pm2_5` y `pm10`, resolución horaria, zona GMT.

## Años de datos disponibles
| Periodo | Origen | Uso en el proyecto |
|---|---|---|
| **2023** (8 760 h) | JSON histórico descargado (`aq_juliaca_2023.json`) | Pipeline batch de U1, entrenamiento y prueba del modelo, replay en streaming |
| **2024** | Descarga por rango de fechas a Open-Meteo (`start_date`/`end_date`), 8 784 horas | Histórico real cargado a Kafka en `atmos-aire-hist-2024` y **validación del modelo** (MAE 0,4762 vs 0,7095, −32,9 %) |
| **2025** | Descarga por rango de fechas a Open-Meteo, 8 760 horas | Histórico real cargado a Kafka en `atmos-aire-hist-2025` y **validación del modelo** (MAE 0,5130 vs 0,6522, −21,3 %) |
| **2026** (1 ene – 20 nov) | Descarga por rango de fechas a Open-Meteo, 7 776 horas | **Validación del modelo** (MAE 0,6335 vs 0,8349, −24,1 %); no se cargó a Kafka |
| **Horas recientes** | API de Open-Meteo, una consulta por hora | Flujo en vivo hacia `atmos-aire` / `atmos-aire-demo` |

Los años 2024 y 2025 son datos reales de la misma fuente, publicados hora por hora con el mismo contrato de evento (`aire.lectura`). Se cargan como **ingesta (Bronze en Kafka)** y, además, se usaron para **validar** el modelo, que se entrenó solo con 2023. Ver [Validación 2024-2026](validacion-2024-2026.md).

## Topics
| Topic | Contenido |
|---|---|
| `atmos-aire` | Telemetría `aire.lectura`: replay de 2023 y lecturas de la API |
| `atmos-negocio` | Eventos de negocio `aire.episodio` |
| `atmos-aire-demo` | Reproducción de la demostración (a ritmo de un evento cada 10 s desde 2023-10-20) y luego la API cada hora |
| `atmos-aire-hist-2024` | Año 2024 completo |
| `atmos-aire-hist-2025` | Año 2025 completo |
| `atmos-aire-int-<runId>` | Un topic por corrida de integración (168 eventos) |

Todos con 1 partición y factor de replicación 1. En conjunto, el broker acumula alrededor de **27 000 mensajes** (se ve en Kafka UI y en el panel «Mensajes por topic» de Grafana).

## Cómo se cargan
| Comando | Qué hace |
|---|---|
| `scripts\cargar_historico.cmd` | Publica 2023 completo en `atmos-aire` (8 760 mensajes); si el topic ya tiene datos, no repite |
| `scripts\cargar_historico_anio.cmd 2024` | Descarga el año indicado desde Open-Meteo y lo publica en `atmos-aire-hist-<año>` |
| `scripts\demo_continuo.cmd` | Crea `atmos-aire-demo`, arranca el job y alimenta un evento cada 10 s |
| `scripts\reiniciar_demo.cmd` | Reinicio limpio del demo: Grafana vuelve a cero, sin duplicados |
| `scripts\detener_demo.cmd` | Detiene el job y el alimentador |

## Calidad de los datos (2023)
| Característica | Valor |
|---|---|
| Filas | 8 760 (365 × 24), sin huecos |
| PM2.5 | media 5,17 · mediana 4,80 · p95 10,80 · máx 35,2 µg/m³ |
| PM10 | mediana 7,10 · p95 16,10 µg/m³ |
| Excedencias del ECA peruano | 0 h en PM2.5 y PM10 |
| Partición temporal | Entrena hasta 2023-10-19 (6 988 h); prueba desde 2023-10-20 (1 747 h) |

## Medallion
| Capa | Contenido | Formato |
|---|---|---|
| Bronze | JSON crudo de Open-Meteo y topics de Kafka | JSON / Kafka |
| Silver | Tipado, hora UTC, descarte de nulos y negativos | DataFrame validado |
| Gold | Agregados diarios y mensuales y filas con features | Parquet particionado por `mes` |
| Speed | `ventanas/` y `evaluacion/` del job de streaming | Parquet |
