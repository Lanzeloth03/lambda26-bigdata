# Mi dimensión U2 (ficha)

**Integrante:** Lanzeloth Vargas Marichi · **Tipo:** predictiva **con** tiempo real (U2, streaming)

| Campo | Contenido |
|---|---|
| **Pregunta** | ¿Cuál será la distancia medida por el sensor en la siguiente lectura (t+1, ≈3 s) y cuánto mejora esa predicción frente a asumir que nada cambia (persistencia)? |
| **Indicador** | `distanciaCm` en vivo (descriptivo) y `prediccionSiguiente` en cm (predictivo); error absoluto medio (MAE) en producción del modelo y de la persistencia |
| **Decisión que habilita** | Detectar de inmediato un cambio de régimen o un fallo del sensor (alerta `AlertaOperativa`), y decidir si el modelo debe reentrenarse cuando su MAE supera al de la persistencia |
| **Instrumento batch** | Acumulación histórica del mismo sensor en Kafka y en Parquet (S9, S10); usada para entrenar y validar |
| **Instrumento streaming** | ESP32 + HC-SR04 → MQTT → Kafka `atmos-real` (3 particiones, clave `sensorId`) |
| **Modelo** | Regresión lineal MLlib sobre `valor` (sin estado), entrenada una vez en batch y **aplicada en vivo** a cada evento dentro de Spark Structured Streaming |
| **Salida** | Panel de Grafana «S12 - Pipeline U2 en vivo»: indicador y predicción actualizándose juntos |

## Requisitos del sistema

1. El sistema debe publicar cada lectura del sensor como un evento JSON validado en el topic `atmos-real`.
2. El sistema debe predecir la lectura siguiente para cada evento válido y medir su error real cuando llega el siguiente valor.
3. El sistema debe agregar los eventos por ventanas de 1 minuto con watermark de 30 s, y tolerar un reinicio sin perder ni repetir lecturas.
4. El sistema debe publicar métricas de flujo, latencia, atraso y calidad del modelo para que Grafana las muestre en vivo.
5. El sistema debe emitir un evento de negocio cuando una lectura salga del rango físico o cambie bruscamente.

## Alcance

**Sí cubre:** ingesta de eventos IoT y de negocio, procesamiento streaming con estado, inferencia en vivo, observabilidad, costos y tablero.

**No cubre:** más de un sensor físico, alta disponibilidad (un solo broker, replicación 1), despliegue en la nube, ni pruebas de carga reales (el escalado es extrapolación lineal de lo medido).

!!! note "Relación con el Proyecto Sello"
    El instrumento sobre el que se valida el pipeline de U2 es el ESP32 con HC-SR04. La fuente del Proyecto Sello (calidad del aire con datos de Open-Meteo) reutilizará el mismo patrón —Kafka, ventanas con watermark, predicción en vivo, Grafana— cambiando la fuente y el esquema del evento; esa transición está por confirmar con el docente.
