# EcoData U2: alerta temprana en tiempo real (Juliaca)

**Proyecto Sello, Unidad 2, Big Data.** Equipo EcoData: Alahin Reyme Ticona Veliz (clima) y Lanzeloth Vargas Marichi (aire).

## Arquitectura Lambda: la ruta Speed
En U1 se construyó la ruta **Batch** (Open-Meteo → limpieza → Parquet → MLlib). En U2 se implementa la ruta **Speed**: las mismas fuentes llegan evento a evento por Kafka, Spark Structured Streaming las procesa con ventanas y watermark, un modelo pronostica la hora siguiente y todo se observa en un tablero.

```
Open-Meteo (aire) ──> atmos-aire  ──┐
                                    ├─> Spark Structured Streaming ─> Pushgateway ─> Prometheus ─> Grafana
Open-Meteo (clima) ─> atmos-clima ──┘             └─> atmos-negocio (aire.episodio, clima.helada)
```

| Dimensión | Responsable | Pregunta | Topic |
|---|---|---|---|
| Aire | Lanzeloth | ¿Se predice el PM2.5 de la próxima hora mejor que la persistencia? | `atmos-aire` |
| Clima | Alahin | ¿Se predice la temperatura de la próxima hora mejor que la persistencia? | `atmos-clima` |

Páginas: [Brief del proyecto](../brief.md) · [Dimensión aire](dimension-aire.md) · [Dimensión clima](dimension-clima.md) · [Operación, escalado y costos](operacion.md).

## Por qué se reformuló la pregunta
La pregunta inicial («¿cuándo superará el umbral?») no tiene casos positivos: en 2023 la norma peruana de PM2.5 nunca se excedió. Se reemplazó por un pronóstico a +1 h comparado con la persistencia, medible en todas las horas.
