# Modelo de series de tiempo (S10)

## Planteamiento

**Pregunta:** dado `valor` (la distancia actual), ¿podemos predecir la lectura del instante siguiente (t+1, ≈ 3 s)?

* **Objetivo:** `siguiente = lead(distanciaCm, 1)`.
* **Feature:** solo `valor`. Es *sin estado*: en streaming cada evento llega solo, y así el mismo modelo sirve en entrenamiento e inferencia.
* **Regla de honestidad:** cualquier modelo debe superar primero a la **persistencia** (`predicción = valor actual`).
* **Split cronológico 80/20** con verificación `max(train) < min(test)`; no hay fuga temporal.

## Validación en batch (conjunto de prueba)

| Modelo | RMSE (cm) | MAE (cm) | RMSE vs persistencia |
|---|---|---|---|
| Persistencia | 0,2016 | 0,1139 | — |
| Regresión lineal | 0,1821 | 0,1230 | **+9,63 %** |
| Random Forest | 5,2121 | — | −2 485,97 % |

* La **LR gana por RMSE** pero tiene **peor MAE** que la persistencia: mejora los errores grandes, no los pequeños.
* El **Random Forest** se degradó por 12 valores atípicos del arranque del sensor.
* Se despliega la LR (liviana y sin estado), guardada como `PipelineModel`; se verificó la diferencia máxima de predicción entre el modelo original y el recargado.

La recta aprendida es `siguiente ≈ 0,77·valor + 27,3`, con punto fijo ≈ 118,6 cm: el modelo «jala» cualquier valor hacia ese nivel.

## Inferencia en vivo

El job aplica el modelo a **cada evento válido** dentro de Spark Structured Streaming y mide el error cuando llega el siguiente valor:

* `MAE modelo = media(|real(t) − predicción hecha en t−1|)`
* `MAE persistencia = media(|real(t) − real(t−1)|)`

## Hallazgo: deriva de régimen

| Medición en vivo (22/10) | Valor |
|---|---|
| MAE del modelo LR | 19,968 cm |
| MAE de la persistencia | 1,254 cm |
| Pares evaluados | 112 |
| *Training/serving skew* (producción / prueba) | 19,968 / 0,1230 ≈ **162×** |

El sensor pasó de ≈ 118,6 cm a ≈ 18 cm; como la LR tira hacia 118,6, su error explotó. Es *concept drift*: un modelo bueno en prueba falla en producción cuando cambia el régimen.

**Mitigación:** `scripts/reentrenar_lr.py` reentrena con las últimas N lecturas, valida con split cronológico contra la persistencia y solo guarda el modelo si no es peor. El tablero lo vigila con el cociente MAE modelo / MAE persistencia.

## Limitaciones

* Un único feature: no usa estacionalidad ni historia.
* Valida sobre pocos miles de eventos de un solo sensor.
* El MAE en producción se calcula desde el arranque del proceso y se reinicia si el job se reinicia.
