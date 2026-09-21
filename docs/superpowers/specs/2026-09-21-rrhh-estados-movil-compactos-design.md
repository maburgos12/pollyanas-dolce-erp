# Resumen compacto de estados de horas extra en móvil

## Problema y objetivo

En la lista de horas extra de RRHH, los cinco resúmenes de estado se apilan en una sola columna cuando la pantalla mide 760 px o menos. En un teléfono ocupan casi toda la primera pantalla y desplazan los registros. El objetivo es conservar visibles los cinco estados y sus conteos, pero reducir la altura del bloque para que el primer registro aparezca antes.

## Decisión aprobada

Usar la opción A elegida por Mauricio: una cuadrícula móvil de dos columnas y tres filas. Pendiente, Autorizado, Rechazado y Pagado ocupan las primeras cuatro celdas; Cancelado ocupa la última fila completa. Se mantienen el orden, los nombres, los números y la distinción visual del primer estado. Los resúmenes son informativos; este cambio no los convierte en filtros ni botones.

## Alcance

- Ajustar únicamente el CSS responsivo del resumen de estados de `rrhh/templates/rrhh/horas_extra_list.html`, en `static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css`.
- Actualizar la versión del enlace a ese CSS en el template para invalidar caché del archivo estático.
- Mantener sin cambios la presentación de escritorio, la tabla/lista de registros, los cálculos, los permisos, las acciones y las respuestas del servidor.
- No modificar modelos, API, migraciones, datos de RRHH ni reglas de negocio.

## Comportamiento y criterios de aceptación

1. En anchos móviles de referencia de 390 px y 320 px, los cinco resúmenes se ven juntos en el orden indicado, sin desbordamiento horizontal ni texto cortado.
2. El bloque de estados pasa de cinco filas a tres; el primer registro queda visiblemente más arriba que en la versión actual.
3. Los conteos mostrados coinciden con los mismos datos del servidor antes y después del cambio.
4. En anchos superiores a 760 px se conserva la disposición existente.
5. La lectura accesible mantiene nombres y cantidades; no se agregan controles aparentes sin función.

## Implementación y validación

La solución es CSS de presentación, con un ajuste de versión del recurso estático. Se revisarán el diff, `manage.py check`, `migrate --check`, pruebas relevantes de RRHH y la página en navegador real a 390 px, 320 px y escritorio, incluyendo consola y solicitudes de red. Como es una interfaz del ERP, el cierre real requiere PR, merge, despliegue por `deploy_web_safe.sh` y verificación de la pantalla servida en producción; una vista local no equivale a esa validación.

## Riesgos y límites

El riesgo principal es que los nombres o conteos no quepan en 320 px o que una regla CSS compartida afecte escritorio. Se evita acotando la regla al bloque y breakpoint existentes, permitiendo envolver texto y comparando las vistas móvil y escritorio. No se introducirán cambios en la lógica de estados para resolver un problema de espacio.
