# Limitaciones y mejoras

## Limitaciones reconocidas
1. **Fuente modelada, no medida.** Open-Meteo/CAMS usa una grilla de ~45 km; no es una estación. Sirve para validar arquitectura y modelo, no para vigilancia sanitaria.
2. **Latencia de la fuente.** Los datos son horarios: el procesamiento es en tiempo real, pero el dato nuevo llega una vez por hora.
3. **Un solo año de entrenamiento (2023) y una ubicación.** Hay estacionalidad (PM10 de enero ≈ 2,4 frente a agosto ≈ 10,0 µg/m³): el modelo debe reentrenarse.
4. **Pregunta reformulada.** «¿Cuándo supera el umbral?» no tenía casos: en 2023 el ECA peruano nunca se excedió. Se cambió a pronóstico a +1 h comparado con persistencia.
5. **Contadores inflados por la demo.** Eventos procesados, horas evaluadas y filas descartadas acumulan repeticiones del replay y de la API; las **métricas de error (MAE)** son las comparables.
6. **Un solo nodo y un broker.** Spark local y Kafka sin replicación: no hay alta disponibilidad.
7. **Pushgateway para un job continuo.** Se mitiga con el semáforo de frescura.
8. **Costos ilustrativos.**
9. **Reentrenamiento manual**, sin detección automática de deriva.

## Mejoras priorizadas
| Prioridad | Mejora | Beneficio |
|---|---|---|
| Alta | Estación real o más fuentes (SENAMHI, sensores de bajo costo) | Dato medido y no solo modelado |
| Alta | Reentrenamiento programado y detección de deriva | Modelo al día con la estación |
| Media | Exportador `/metrics` propio en lugar de Pushgateway | Detección de caída más fiable |
| Media | Kafka con 3 brokers y Spark en clúster | Alta disponibilidad |
| Baja | Notificaciones (correo/Telegram) desde las alertas | No depender de mirar el tablero |
