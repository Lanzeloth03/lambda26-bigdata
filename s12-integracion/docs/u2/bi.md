# BI/ML distribuido: KPIs y predicción (S11 y S12)

El tablero muestra, **en la misma sesión**, los KPIs del flujo y la predicción del modelo de series de tiempo.

## KPIs del flujo

| KPI | Definición | Panel |
|---|---|---|
| Volumen | eventos válidos procesados | «Eventos IoT procesados» |
| Calidad | proporción de `isValid` | «Eventos inválidos» |
| Latencia | promedio y p95 de `latencyMs` | «Latencia evento → procesamiento» |
| Flujo | filas/s de entrada y proceso | «Entrada Kafka vs procesamiento Spark» |
| Negocio | alertas y órdenes por severidad | «Eventos de negocio» |

## KPIs de la predicción

| KPI | Definición | Panel |
|---|---|---|
| Real vs predicción | valor actual y predicción alineada | «distanciaCm real vs predicción» |
| MAE en producción | modelo y persistencia | «MAE modelo / persistencia» |
| Cociente | MAE modelo ÷ MAE persistencia | «Modelo / persistencia (x)» |
| Deriva | cociente > 1 sostenido | color rojo del panel |

## Dos tableros

* **S11:** lo calcula un notebook sobre los Parquet de S9 y S10 y lo publica una sola vez al Pushgateway: valores puntuales.
* **S12:** lo calcula el job persistente y se actualiza cada lote: series en vivo. Es el que se usa en la demo.

## Decisión que habilita

Si el cociente MAE modelo / persistencia supera 1 de forma sostenida, el modelo no aporta frente a «asumir que nada cambia»: se reentrena o se usa la persistencia hasta hacerlo.
