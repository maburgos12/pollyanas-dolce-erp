# Captura compacta de conteos por sucursal

## Resultado

La captura móvil mostrará un solo tipo de artículo a la vez: Productos o Insumos. Cada artículo ocupará un renglón compacto con nombre, código, unidad oficial y campo de cantidad alineado a la derecha. La incidencia permanecerá contraída debajo del renglón.

## Búsqueda de excepciones

Durante un conteo abierto, la persona podrá buscar por nombre o código dentro del catálogo activo. Los resultados excluirán artículos ya incluidos y mostrarán su unidad oficial. “Agregar” guardará atómicamente las cantidades visibles y añadirá una línea a la ronda actual, evitando pérdida de borradores, duplicados y artículos sin unidad verificada.

## Contratos

- Point sigue siendo la fuente del catálogo de productos; Insumo y su unidad base son la fuente de insumos.
- No se infiere una clasificación distinta a producto o insumo.
- Agregar una línea registra un evento inmutable y aumenta la versión del conteo.
- Solo la persona con permiso de captura puede buscar y agregar mientras el conteo esté en CAPTURA o RECONTEO.
- El conteo continúa siendo ciego y nunca modifica existencias.

## Estados y accesibilidad

Los controles serán operables con teclado, tendrán nombres accesibles y áreas táctiles de al menos 44 px. La pantalla mostrará resultado vacío, error y carga sin ocultar la captura. Los filtros conservarán el tipo activo después de guardados asíncronos.

## Validación

Se probarán permisos, unidad verificada, duplicados, idempotencia, conservación de lecturas, filtrado de catálogo y renderizado por tipo. La pantalla se verificará a 390 px en navegador real, incluyendo búsqueda, agregado y ausencia de desplazamiento horizontal.
