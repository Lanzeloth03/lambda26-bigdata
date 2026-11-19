# 8. Equipo y aporte individual

El equipo EcoData nació con dos integrantes. Por indicación del docente, **cada integrante construye de extremo a extremo su propia dimensión** y la sustenta por separado.

| Integrante | Dimensión | Repositorio |
|---|---|---|
| **Lanzeloth Vargas Marichi** | **Aire** (PM2.5/PM10, Open-Meteo CAMS, Juliaca) | este repositorio |
| Alahin Reyme Ticona Veliz | Clima (temperatura) | repositorio propio |

## Aporte individual de Lanzeloth (verificable)
| Unidad | Entregable | Evidencia en el repositorio |
|---|---|---|
| U1 | Pipeline batch PySpark (S2–S4): Bronze→Silver→Gold Parquet por mes, 8 modelos comparados, GBT ganador | `u1-aire/`, `u1-aire_corrida/` (Gold, modelo, figuras) |
| U1 | Deck de arquitectura y de resultados | `EcoData_S1_Arquitectura_Aire.pptx`, `EcoData_U1_Aire_Lanzeloth.pptx` |
| U2 | Productor Open-Meteo→Kafka, streaming con ventanas/watermark, inferencia +1 h, métricas, alertas, tablero | `s12-aire/`, `obs/prometheus/alerts.yml`, `s12-aire/grafana/` |
| U3 | Hardening de compose, scripts de arranque, cuaderno de integración con compuertas y manifiesto, tablero U3 | `kafka/`, `obs/`, `scripts/`, `u3-integracion/` |
| U3 | MkDocs, Brief, matriz, informe, guion de demo | `docs/`, `mkdocs.yml` |

Los commits del repositorio están a nombre de `lanzeloth` y se pueden listar con `git log --author=lanzeloth --oneline`. El trabajo de las sesiones S6–S11 (pipeline ESP32/DHT11) también está en el historial.

!!! note "Uso de asistentes de IA"
    Parte del código y de la documentación se elaboró con asistencia de Claude (Anthropic); los commits que lo reflejan llevan la línea `Co-Authored-By`. Las decisiones de diseño, las ejecuciones y los resultados son verificables en el repositorio y se explican en la sustentación.
