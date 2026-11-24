# 9. Repositorio y estándares

**URL:** <https://github.com/Lanzeloth03/lambda26-bigdata> · **Sitio MkDocs:** <https://lanzeloth03.github.io/lambda26-bigdata/>

## Topics académicos (Settings → About → Topics)
```
campus-juliaca  semestre-2026-2  linea-cdia  tipo-ps  bigdata  seccion-g1  grupo-5-ecodata-aire
```
Reemplaza `<n>` por el número de grupo asignado por el docente (patrón `grupo-<número>-<nombre-proyecto>`).

## Estructura
Ver [Arquitectura → Carpetas principales](arquitectura.md#carpetas-principales). Notebooks numerados por sesión (`s08…`, `S2–S4`, `13_…`), scripts en `scripts/`, infraestructura en `kafka/` y `obs/`.

## Commits
Mensajes en español, imperativo y con el alcance (`U3 Integración: …`, `Hardening: …`). Un cambio lógico por commit; los de hardening se separan para poder revertirlos uno a uno.

## Criterios de reproducibilidad
| Criterio | Cumplimiento |
|---|---|
| Versiones fijadas | imágenes con tag; `requirements-docs.txt` con rangos |
| Un comando de arranque | `scripts\levantar.cmd` |
| Configuración fuera del código | `.env` / `.env.example` |
| Datos incluidos o regenerables | `aq_juliaca_2023.json` (240 KB) versionado |
| Evidencia automática | manifiestos JSON + `resumen_manifiestos.py` |
| Documentación publicada | MkDocs + GitHub Actions (Pages) |
| Sin secretos | `.env`, `secrets.h` ignorados |

## Publicar el sitio
Settings → Pages → *Source: GitHub Actions*. Cada `push` a `main` que toque `docs/` reconstruye con `mkdocs build --strict`.
