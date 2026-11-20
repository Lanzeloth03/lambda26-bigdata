# Limitaciones y mejoras

## Limitaciones
1. **Dato modelado, no medido** (CAMS, ≈ 45 km): no representa un punto concreto de la ciudad ni sirve para decisiones sanitarias.
2. **Un punto y datos históricos**: se conoce el desempeño en 2023 (entrenamiento y prueba) y se cargaron 2024 y 2025 solo como volumen real en Kafka; no se validó contra otra ubicación.
3. **Alerta relativa:** el episodio usa el p95 de 2023 (10,5 µg/m³); la norma peruana nunca se excedió en 2023, por eso la pregunta es un pronóstico.
4. **Deriva estacional:** PM10 de enero ≈ 2,4 frente a agosto ≈ 10,0 µg/m³; el modelo se entrenó hasta octubre.
5. **La fuente es horaria:** el procesamiento es continuo, pero el dato no es de baja latencia.
6. **Pushgateway congela la última métrica** si el job muere; se cubre con el semáforo de frescura y la alerta de 10 min.
7. **Kafka sin volumen persistente:** al recrear los contenedores se pierden los datos de los topics.
8. **Un solo nodo y un broker:** sin alta disponibilidad; sin TLS/SASL ni autenticación en Pushgateway (laboratorio).
9. **Semántica al menos una vez**, no exactly-once.
10. **Los contadores de la demo incluyen reproducciones:** solo el MAE es comparable.

## Mejoras priorizadas
| Prioridad | Mejora | Valor | Esfuerzo |
|---|---|---|---|
| 1 | Volumen persistente para Kafka y Pushgateway | Los datos sobreviven a un reinicio | Bajo |
| 2 | Reentrenamiento programado con alerta de deriva automática | Mantiene el MAE en producción | Medio |
| 3 | Fuente real: sensor de bajo costo o estación calibrada contra CAMS | Pasa de «validar arquitectura» a «medir» | Alto |
| 4 | `/metrics` propio en vez de Pushgateway | Quita la métrica congelada | Medio |
| 5 | Logs estructurados en JSON (nivel, componente, id de lote) | Mejor trazabilidad | Bajo |
| 6 | Varias ubicaciones (claves y particiones distintas) | Escala | Alto |
| 7 | Cruzar PM2.5 con temperatura y humedad | Nuevas variables explicativas | Medio |
