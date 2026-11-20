# 5. Módulos y funcionalidades

| Módulo | Archivo | Qué hace | Cómo responde al problema |
|---|---|---|---|
| Contrato y features | `s12-aire/scripts/aire_comun.py` | Define el esquema del evento y construye las 8 features (`l0, d1, d3, d24, m24, pm10d, hs, hc`) | Una sola fuente de verdad en entrenamiento y servicio |
| Entrenamiento | `entrenar_pm25.py` | Split cronológico 80/20; compara persistencia, lineal y GBT; **solo guarda el modelo si gana a la persistencia** | Garantiza que lo desplegado es mejor que el baseline |
| Productor | `openmeteo_a_kafka.py` | Publica el JSON histórico (replay) o la API en vivo a `atmos-aire`; `--tardio` para probar el watermark; `--hasta` para ventana fija | Simula el flujo de sensores de forma realista |
| Streaming | `s12_streaming_aire.py` | Dos consultas: `ventanas` (6 h/watermark 2 h) e `inferencia` (+1 h, MAE modelo vs persistencia, episodios) | Pronóstico y alerta casi en tiempo real |
| Integración | `u3-integracion/13_integracion_dataops.ipynb` | Previas → calidad → streaming → MAE → Prometheus → manifiesto → comparación | Demuestra que el sistema completo funciona y es reproducible |
| Resumen | `scripts/resumen_manifiestos.py` | Convierte los `manifest_*.json` en la tabla de [Corridas](evidencias/corridas.md) | Evidencia generada, no escrita a mano |
| Alertas | `obs/prometheus/alerts.yml` | `AireSinEventosNuevos`, `AireModeloNoSuperaPersistencia`, `EpisodioDeAire`, `DescartesPorWatermarkCreciendo`, `PushgatewayCaido` | Operación sin mirar la pantalla |
| Arranque | `scripts/*.cmd` | Orden de arranque, espera por salud, topics, chequeo | Reproducibilidad |

## Métricas publicadas (prefijo `lambda26_aire_`)
`pm25_ultimo`, `pred_pm25_proxima_hora`, `mae_modelo`, `mae_persistencia`, `mejora_pct`, `n_evaluadas`, `eventos_total`, `episodios_total`, `umbral_p95`, `lote_id`, `ultima_hora_evento_seconds`, `filas_descartadas_watermark_total`, `watermark_seconds`, `ventana_*`. Las corridas de integración usan el job `lambda26_aire_int` para **no pisar** el tablero del demo; el cuaderno añade `lambda26_aire_int_gates_ok`, `_gates_total`, `_duracion_total_seconds`.
