# Estimación de costos y criterios de escalado (S9)

!!! info "Precios de referencia"
    AWS us-east-1: t3.medium US$ 0,0416/h; S3 Standard US$ 0,023/GB-mes. Son valores de referencia y deben verificarse en la página de AWS antes de usarlos como presupuesto.

## Volumen medido

| Medida | Valor |
|---|---|
| Fuente | 1 sensor, 1 evento cada 3 s (≈ 0,33 eventos/s) |
| Crecimiento del Parquet | 1 177 filas/h y 2,65 MB/h (≈ 1,935 GB/mes, cota alta por archivos pequeños) |
| Latencia promedio / p95 | 3 596 ms / 7 004 ms |
| Lag | 0–1 |

## Costo base mensual

| Concepto | Cálculo | US$/mes |
|---|---|---|
| Cómputo | 3 × t3.medium × 0,0416 × 730 h | 91,10 |
| Almacenamiento | 1,935 GB × 0,023 | 0,045 |
| **Total base** (sin red ni mano de obra) | | **91,15** |

El cómputo es prácticamente todo el costo. **Costo unitario:** 91,15 / (1 177 × 730 ≈ 859 000 eventos/mes) ≈ **US$ 106 por millón de eventos** con un sensor.

## Cuellos de botella detectados

1. Una sola partición activa (todos los eventos tienen la misma clave).
2. Un solo broker con replicación 1: sin tolerancia a fallos.
3. Latencia dominada por el trigger (5 s) y no por el cómputo.
4. Muchos archivos Parquet pequeños (uno por micro-lote).
5. El estado del job (3 consultas) comparte CPU y memoria con Jupyter si hay kernels abiertos.

## Plan de escalado

| Escalón | Carga | Cambios | Almacenamiento | Costo |
|---|---|---|---|---|
| Hoy | 1 sensor · 0,33 ev/s | Sin cambios | 1,9 GB/mes | US$ 91,15 |
| 100 sensores | ≈ 33 ev/s | Claves distintas para repartir 3 particiones; trigger ≥ 10 s; compactar Parquet | ≈ 193 GB/mes (cota alta), ≈ US$ 4,5 | ≈ US$ 95,6 **si** el mismo cómputo alcanza (hipótesis a probar); ≈ US$ 1,1 por millón de eventos |
| 1 000 sensores | ≈ 333 ev/s | 3 brokers con replicación 3; 6–12 particiones; Spark en clúster | ≈ 1,9 TB/mes (cota alta) | no estimado: requiere prueba de carga |

Son **extrapolaciones lineales de lo medido, no pruebas de carga**.

## Criterios de escalado (cuándo actuar)

| Señal en Grafana | Umbral | Acción |
|---|---|---|
| Atraso de offsets de Spark | > 10 durante 2 min | más particiones y más ejecutores |
| Filas/s procesadas < filas/s de entrada | sostenido 5 min | subir cómputo o reducir el trabajo por lote |
| p95 de latencia | > 12 000 ms | revisar trigger y red aguas arriba |
| Filas de estado | crecimiento sin techo | revisar watermark (debe acotar el estado) |
| Una partición concentra todo el tráfico | siempre con 1 sensor | claves distintas antes de añadir particiones |
