# Informe del proyecto — EcoData Aire

## 1. Portada
**Proyecto:** EcoData Aire — alerta temprana de calidad del aire en Juliaca · **Curso:** Big Data · **Sección:** G1 · **Integrante:** Lanzeloth Vargas Marichi · **Docente:** Abel Ángel Sullón Macalupú · **Semestre:** 2026-2 · **Universidad Peruana Unión, campus Juliaca.**

## 2. Resumen del proyecto
Juliaca carece de monitoreo accesible de material particulado. EcoData Aire ingiere PM2.5 y PM10 horarios de Open-Meteo (modelo CAMS, 2023, 8 760 h), los valida y los almacena en un Gold Parquet particionado por mes (batch), y los reproduce evento a evento por Kafka hacia Spark Structured Streaming con ventanas de 6 h y watermark de 2 h. Un modelo GBT de MLlib pronostica el PM2.5 de la próxima hora y se compara con la persistencia: en prueba cronológica reduce el MAE 32,5 % (0,4635 vs 0,6871 µg/m³) y en streaming en vivo 25,5 % (0,422 vs 0,567). Las métricas llegan a Grafana por Pushgateway y Prometheus, con alertas. La unidad 3 integra todo con servicios con *healthcheck*, credenciales por variables, un cuaderno con 19 compuertas que genera un manifiesto por corrida, y una revisión de *hardening* con prueba de no regresión. El dato es modelado (~45 km), por lo que el valor es de arquitectura y método, no de vigilancia sanitaria.

## 3. Competencia y alcance
**CE042 Construye Dataset:** Bronze→Silver→Gold, contrato del evento, calidad. **CE043 Genera Modelos:** 8 modelos comparados, GBT en batch y en streaming. **CE044 Analiza y Define Estrategias:** observabilidad, costos, decisiones y riesgos. Alcance y exclusiones: [Brief](brief.md).

## 4. Matriz de trazabilidad
Ver [Matriz de trazabilidad](trazabilidad.md) (12 filas, una por indicador, con evidencia).

## 5. Problema y datos
Necesidad: anticipar PM2.5 y marcar episodios. Fuente: Open-Meteo Air Quality (CAMS), lat −15,5 / lon −70,13. Restricciones: dato modelado, un año, una ubicación. Detalle y contrato: [Datos](datos.md).

## 6. Arquitectura Big Data
Lambda; ingesta Kafka; procesamiento Spark (batch y streaming); almacenamiento Parquet; consumo Grafana; observabilidad Prometheus. Diagramas y decisiones: [Arquitectura](arquitectura.md), [Decisiones](decisiones.md).

## 7. Procesamiento batch
S2 (transformaciones perezosas), S3 (calidad: 0 nulos, 0 duplicados, 0 negativos, 0 huecos; 12 partes; poda de partición verificada con el plan), S4 (8 modelos). Evidencia: `u1-aire_corrida/`. Resumen: [Producto U1](u1-producto.md).

## 8. Ingesta y streaming
Productor Python → `atmos-aire`; Spark: consulta de ventanas y consulta de inferencia (`foreachBatch`), evento de negocio `aire.episodio`. Resultado en vivo: MAE 0,422 vs 0,567 (761 h). Detalle: [Producto U2](u2-producto.md).

## 9. Observabilidad y costos
Métricas `lambda26_aire_*`, 2 tableros, 8 alertas; costo de referencia ≈ US$ 30/mes en una VM mínima. [Observabilidad y costos](observabilidad-costos.md).

## 10. Resultados BI/ML
| Modelo | MAE test (µg/m³) |
|---|---|
| Persistencia | 0,6871 |
| Regresión lineal | 0,7419 (peor) |
| Random Forest | 0,4780 |
| **GBT depth 3** | **0,4635 (−32,5 %)**, R² 0,9261 |

Interpretación para decisiones: el pronóstico a +1 h es utilizable como señal de alerta temprana relativa; no sustituye una medición.

## 11. Integración y DataOps
Healthchecks (5 de 6 servicios), `service_healthy`, `.env`, `levantar.cmd`, cuaderno 13 (19 compuertas: previas, calidad, plausibilidad p50/p95, streaming con modelo, MAE de producción, confirmación en Prometheus, manifiesto, comparación de ≥ 2 corridas). Hardening con 14 controles, 7 pruebas de no regresión, 10 riesgos aceptados y bitácora de 9 hallazgos: [Hardening](hardening.md), [No regresión y riesgos](noregresion-riesgos.md), [Bitácora](bitacora.md), [Corridas](evidencias/corridas.md).

## 12. Repositorio y documentación
Repositorio: <https://github.com/Lanzeloth03/lambda26-bigdata> · MkDocs: <https://lanzeloth03.github.io/lambda26-bigdata/> · Topics y estándares: [Repositorio](repositorio.md).

## 13. Aporte individual
[Equipo y aporte individual](equipo.md): U1 completa, U2 completa (productor, streaming, modelo, alertas, tablero), U3 (hardening, scripts, cuaderno 13, documentación).

## 14. Limitaciones y mejoras
Limitaciones, mejoras priorizadas y **riesgos aceptados con justificación**: [Limitaciones](limitaciones.md), [Riesgos](noregresion-riesgos.md).
