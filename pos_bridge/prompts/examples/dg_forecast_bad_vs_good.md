# Ejemplo DG: forecast de gasto en insumos

## Pregunta real

`que tanto gasto en insumos haremos en lo que resta del mes de abril?`

## Respuesta mala

Problema:
- Se refugia demasiado en que no hay KPI perfecto.
- No separa gasto comprometido vs forecast operativo.
- No ayuda a decidir.

Ejemplo de respuesta mala:

`No puedo confirmar un gasto proyectado confiable porque compras_ready = false y no hay ordenes ni solicitudes.`

## Por que esta mal

- Si no hay ordenes ni solicitudes, eso si dice algo util: lo comprometido hoy es 0.
- Eso no significa que el gasto esperado real tambien sea 0.
- La IA debio ofrecer el siguiente calculo operativo, no detenerse ahi.

## Respuesta buena esperada

`Resumen ejecutivo:`
`Al 13 de abril de 2026, el gasto comprometido en ERP para insumos del resto del mes es $0 porque no hay ordenes ni solicitudes registradas. Eso no implica que el gasto esperado real sea cero; solo significa que hoy no hay compras comprometidas capturadas.`

`Hecho auditado:`
- `compras_total = 0`
- `compras_ready = false`
- `ordenes de compra encontradas = 0`
- `solicitudes de compra encontradas = 0`

`Estimado operativo:`
- `Comprometido en ERP: $0`
- `Forecast real: aun no calculado con base operativa`
- `Para estimarlo correctamente debo cruzar ventas recientes, inventario actual, stock bajo y consumo de insumos`

`Riesgo / interpretacion:`
- `Hoy no hay gasto comprometido visible en ERP`
- `Si si habra necesidad de reposicion este mes, el riesgo es subcaptura o falta de planeacion de compras`

`Siguiente accion recomendada:`
- `Calcular de inmediato el estimado del resto de abril usando ventas recientes + inventario actual + alertas de stock bajo`

## Regla extraida

En preguntas de DG:
- primero decir el dato auditado
- despues separar el estimado operativo
- nunca responder solo con la falta del KPI
