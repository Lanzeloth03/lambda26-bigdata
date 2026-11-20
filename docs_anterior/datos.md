# 6. Datos

## Fuente
**Open-Meteo Air Quality API** (modelo **CAMS**), lat −15,5 / lon −70,13, variables `pm2_5` y `pm10`, resolución horaria, zona GMT, año 2023. **Dato modelado en grilla de ~45 km, no una estación.**

| Característica | Valor |
|---|---|
| Filas | 8 760 (365 × 24), sin huecos |
| PM2.5 | media 5,17 · p50 4,80 · p95 10,80 · máx 35,2 µg/m³ |
| PM10 | p50 7,10 · p95 16,10 µg/m³ |
| Excedencias ECA Perú | 0 h (PM2.5 y PM10); guía OMS PM10 45 µg/m³: 4 h |
| Estacionalidad | PM10 enero ≈ 2,4 vs agosto ≈ 10,0 µg/m³ |

## Modelo de almacenamiento (Medallion)
| Capa | Contenido | Formato |
|---|---|---|
| Bronze | JSON crudo de Open-Meteo (`aq_juliaca_2023.json`) | JSON |
| Silver | Tipado, hora UTC, descarte de nulos/negativos | DataFrame validado |
| Gold | Agregados diarios/mensuales y filas con features | **Parquet particionado por `mes`** (`u1-aire_corrida/gold/aire_mensual`) |
| Speed | `ventanas/` y `evaluacion/` del job | Parquet (no se versiona: `*.parquet` está en `.gitignore`) |

El particionado por mes se verificó con el plan de ejecución: la consulta de un mes lee solo `mes=2023-06` (*PartitionFilters*).

## Contrato del evento (Kafka)
```json
{"tipoEvento":"aire.lectura","sensorId":"openmeteo-cams-juliaca","pm25":6.5,"pm10":9.4,
 "origen":"openmeteo-replay|openmeteo-cams|demo-tardio","timestamp":1701388800000}
```
`timestamp` = hora del dato (tiempo de evento, ms). Evento de negocio: `aire.episodio` (PM2.5 > p95 de entrenamiento = 10,5 µg/m³) en el topic `atmos-negocio`.

## Calidad y partición temporal
* Reglas del batch: sin duplicados de hora, sin negativos, continuidad horaria ≥ 99 %, mediana plausible.
* Split **cronológico** (entrena hasta 2023-10-19; prueba desde 2023-10-20: 6 988 / 1 747 filas). Nunca aleatorio: es una serie de tiempo.
