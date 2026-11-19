# 10. Limitaciones y mejoras

## Limitaciones
1. **Dato modelado, no medido** (CAMS, ~45 km): no representa un punto concreto de la ciudad ni sirve para decisiones sanitarias.
2. **Un año y un punto**: no se conoce el desempeño en otros años ni en otra ubicación.
3. **Alerta relativa**: el *episodio* usa el p95 de 2023 (10,5 µg/m³); la norma peruana nunca se excedió en 2023, por eso se reformuló la pregunta.
4. **Deriva estacional**: PM10 de enero ≈ 2,4 vs agosto ≈ 10,0 µg/m³; el modelo se entrenó hasta octubre.
5. **Escala**: un broker, un sensor lógico; el `collect()` en el driver no escala a miles de sensores.
6. **Seguridad de laboratorio**: sin TLS/SASL, sin autenticación en Pushgateway (ver [riesgos](noregresion-riesgos.md)).
7. **La integración con ventana fija no ejercita el watermark** (ver H9).

## Mejoras priorizadas
| Prioridad | Mejora | Valor | Esfuerzo |
|---|---|---|---|
| 1 | Reentrenamiento programado + alerta de deriva automática | mantiene el MAE en producción | medio |
| 2 | Fuente real: sensor PM2.5 de bajo costo o estación SENAMHI, calibrado contra CAMS | pasa de «validar arquitectura» a «medir» | alto |
| 3 | CI que ejecute el cuaderno 13 con `FUENTE=archivos` en cada commit | detecta regresiones sin Docker | bajo |
| 4 | Persistencia y autenticación (Kafka, Pushgateway) | cierra R2/R3/R7 | medio |
| 5 | Inferencia distribuida y varios sensores (claves distintas, más particiones) | escala | alto |
| 6 | Importar los tableros por *provisioning* de Grafana | arranque 100 % sin pasos manuales | bajo |
