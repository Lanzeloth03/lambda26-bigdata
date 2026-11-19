# Decisiones técnicas y su costo

| # | Decisión | Alternativa descartada | Por qué | **Costo asumido** |
|---|---|---|---|---|
| D1 | Pregunta de **pronóstico a +1 h vs persistencia** | «¿cuándo se supera el ECA?» | En 2023 el ECA nunca se excedió: no hay casos positivos | No se emite una alerta normativa; el *episodio* es relativo (p95) |
| D2 | **GBT con pérdida absoluta**, objetivo = delta | Regresión lineal | El lineal minimiza error cuadrático y fue **peor que la persistencia** en MAE (0,742 vs 0,687) | Menos interpretable y más costoso de entrenar |
| D3 | **Lambda** con una sola función de features | Kappa | Hay histórico anual para entrenar/auditar y flujo continuo | Dos rutas que mantener; se mitiga con `aire_comun` |
| D4 | **Topic por corrida** en la integración | Reutilizar `atmos-aire` | Dos corridas no se mezclan; comparación justa | Topics que se acumulan: limpiar de vez en cuando |
| D5 | **Job de Pushgateway propio** (`lambda26_aire_int`) | Mismo job que el tablero | No pisar el tablero en vivo (H6) | Hay que importar dos tableros |
| D6 | **Previas bloqueantes + PL2 que solo advierte** | Todo bloqueante | Un p95 alto puede ser un episodio real; bloquearlo ocultaría datos legítimos | Una advertencia puede pasar inadvertida si nadie lee la salida |
| D7 | **Inferencia en `foreachBatch`** con `collect()` | UDF/transformación distribuida | 1 evento/hora: simple y correcto | No escala a miles de sensores (R10) |
| D8 | **Puertos solo en loopback** | Publicar en todas las interfaces | Reduce la superficie de red | No se puede entrar desde otro equipo de la LAN sin cambiar el compose |
