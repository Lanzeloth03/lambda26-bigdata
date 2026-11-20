# EcoData Aire — Juliaca

**Curso:** Big Data (UPeU, campus Juliaca, sección G1, semestre 2026-2) · **Línea:** Ciencia de Datos e IA · **Docente:** Abel Ángel Sullón Macalupú
**Autor:** Lanzeloth Vargas Marichi · **Repositorio:** [Lanzeloth03/lambda26-bigdata](https://github.com/Lanzeloth03/lambda26-bigdata)

## Problema
En Juliaca la calidad del aire (material particulado PM2.5 y PM10) no se monitorea con una red de estaciones accesible. Se necesita un sistema que ingiera lecturas hora a hora, las valide, **pronostique el PM2.5 de la próxima hora** y avise cuando se entra en un episodio de contaminación relativa.

## Solución
Un sistema Big Data con **arquitectura Lambda**:

* **Batch (U1):** Open-Meteo/CAMS 2023 → Bronze → Silver → **Gold en Parquet particionado por mes** → modelo MLlib (GBT) que supera a la persistencia en 32,5 %.
* **Speed (U2):** los mismos datos llegan evento a evento por **Kafka**; **Spark Structured Streaming** aplica ventanas de 6 h con watermark de 2 h y corre el modelo en vivo; métricas → **Pushgateway → Prometheus → Grafana**.
* **Integración y DataOps (U3):** servicios con *healthcheck* y espera por dependencias, credenciales por variables de entorno, un **cuaderno de integración con compuertas y manifiesto de ejecución**, revisión de *hardening* con prueba de no regresión.

!!! warning "Honestidad sobre el dato"
    La fuente es **PM2.5/PM10 modelado por CAMS** en una grilla de ~45 km (no una estación). Sirve para validar la arquitectura y el modelo, **no para vigilancia sanitaria**.

## Pregunta central
> ¿Un modelo entrenado con las últimas horas de PM2.5 y PM10 de Juliaca predice el PM2.5 de la próxima hora **mejor que repetir la última hora** (persistencia)?

**Respuesta medida:** sí. Batch: MAE 0,4635 vs 0,6871 µg/m³ (−32,5 %). Streaming en vivo: MAE 0,422 vs 0,567 (−25,5 %, 761 h evaluadas). Ver [Pruebas y evidencias](pruebas.md).

## Mapa de la documentación
| Quiero… | Ir a |
|---|---|
| Levantar y probar todo | [Instalación y ejecución](ejecucion.md) → [Uso](uso.md) |
| Entender el diseño | [Arquitectura](arquitectura.md), [Módulos](modulos.md), [Datos](datos.md) |
| Verificar que funciona | [Pruebas](pruebas.md), [Corridas](evidencias/corridas.md), [Hardening](hardening.md) |
| Ver la demo | [Demo y plan B](demo.md) |
| Ver el cumplimiento académico | [Brief](brief.md), [Matriz de trazabilidad](trazabilidad.md), [Informe](informe.md) |
| Ver decisiones, equipo, repositorio y límites | [Decisiones](decisiones.md), [Equipo](equipo.md), [Repositorio](repositorio.md), [Limitaciones](limitaciones.md), [Observabilidad y costos](observabilidad-costos.md) |
