# Limitaciones y mejoras

## Limitaciones reconocidas
1. **Fuente modelada.** Open-Meteo/CAMS usa una grilla de ~45 km y entrega datos horarios.
2. **Latencia de la fuente.** Los datos son horarios: el procesamiento es en tiempo real, pero el dato nuevo llega una vez por hora.
3. **Entrenamiento con un solo año (2023) y una ubicación.** El modelo se entrenó con 2023. Los años 2024, 2025 y 2026 se usaron para validarlo (ver [Validación 2024-2026](validacion-2024-2026.md)): sigue ganando a la persistencia, pero con una ventaja menor en 2025 y una excepción en noviembre de 2025. Hay estacionalidad (PM10 de enero ≈ 2,4 frente a agosto ≈ 10,0 µg/m³): el modelo debe reentrenarse.
4. **Pregunta reformulada.** «¿Cuándo supera el umbral?» no tenía casos: en 2023 el ECA peruano nunca se excedió. Se cambió a pronóstico a +1 h comparado con persistencia.
5. **Kafka sin volumen persistente.** Al recrear los contenedores se pierden los datos de los topics; se recargan con `cargar_historico.cmd`.
6. **Un solo nodo y un broker.** Spark local y Kafka sin replicación: no hay alta disponibilidad.
7. **Pushgateway para un job continuo.** Se mitiga con el semáforo de frescura.
8. **Reentrenamiento manual**, sin detección automática de deriva.

## Mejoras priorizadas
| Prioridad | Mejora | Beneficio |
|---|---|---|
| Alta | Estación real o más fuentes (SENAMHI, sensores de bajo costo) | Dato medido y no solo modelado |
| Alta | Reentrenamiento programado y detección de deriva | Modelo al día con la estación |
| Media | Exportador `/metrics` propio en lugar de Pushgateway | Detección de caída más fiable |
| Alta | Volumen persistente para Kafka | Los datos sobreviven a un reinicio |
| Media | Reentrenar con 2023-2025 y validar en 2026 | Recuperar la ventaja que se reduce en 2025 y reducir la deriva |
| Media | Kafka con 3 brokers y Spark en clúster | Alta disponibilidad |
| Baja | Notificaciones (correo/Telegram) desde las alertas | No depender de mirar el tablero |
