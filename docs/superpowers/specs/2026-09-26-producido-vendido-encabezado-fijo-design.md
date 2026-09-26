# Encabezado fijo de Producido vs Vendido

## Objetivo

Mantener visible el significado de cada columna mientras se revisan las filas del reporte y hacer que la alineación del encabezado coincida con la alineación de sus datos.

## Diagnóstico confirmado

- La tabla usa una sola estructura HTML y sus anchos de encabezado y cuerpo ya coinciden.
- Una regla general de `.table thead th` tiene más prioridad que `.text-end` y `.text-center`, por lo que los encabezados aparecen alineados a la izquierda aunque las cifras estén a la derecha o centradas.
- El encabezado tiene `position: static`; por eso desaparece cuando se recorre la tabla.

## Diseño

- Conservar la tabla, columnas, datos, filtros y exportaciones actuales.
- Dar al contenedor de la tabla un área de desplazamiento vertical limitada por el viewport, manteniendo también el desplazamiento horizontal.
- Aplicar `position: sticky` a las celdas del encabezado dentro de ese contenedor, con fondo opaco, borde y nivel de apilamiento suficiente para que los datos no se transparenten.
- Reforzar específicamente en este reporte la alineación derecha de columnas numéricas y la alineación centrada de porcentaje y estado.
- Añadir `scope="col"` a los encabezados para conservar una asociación semántica clara.
- Actualizar la versión del CSS en `base.html` para que los navegadores reciban el cambio después del despliegue.

## Validación

- Prueba automatizada que confirme las clases semánticas, el encabezado fijo y las reglas específicas de alineación.
- Verificación visual en navegador a ancho de escritorio y móvil.
- Verificación de que el encabezado permanece visible al desplazar verticalmente la tabla y conserva el mismo desplazamiento horizontal que sus celdas.

## Fuera de alcance

- Cálculos, datos, periodos, fuentes Point, filtros y exportaciones.
- Reordenamiento, ocultamiento o renombrado de columnas.
- Rediseño general del módulo.
