# Diseño: Panel de acuerdos con jerarquía por tipo

## Objetivo

Reorganizar el Panel de acuerdos de Dirección General para que la primera decisión sea el tipo de trabajo: **Minutas**, **Compromisos** o **Proyectos**. Después de elegir un tipo, la pantalla mostrará exclusivamente los estados, conteos, responsables y acuerdos de esa categoría.

La pantalla deja de presentar conteos globales como `Vencidos 352` o `Activos 399` que mezclan los tres tipos. Los estados pasan a depender del tipo seleccionado.

## Problema observado

La pantalla actual usa el estado como primer nivel de navegación. Al seleccionar `Vencidos` o `Activos`, la lista combina Minutas, Compromisos y Proyectos. Aunque existe un filtro técnico por tipo, queda escondido en `Filtros y auditoría` y no organiza la experiencia principal.

Esto dificulta responder preguntas operativas simples, por ejemplo:

- cuántas Minutas están vencidas;
- qué Compromisos siguen activos;
- qué Proyectos están por aprobar;
- qué responsables concentran pendientes dentro de una categoría concreta.

## Dirección aprobada

La jerarquía visible será:

1. Tipo: Minutas, Compromisos o Proyectos.
2. Estado del tipo seleccionado: Vencidos, Activos, Por aprobar, Prórrogas o Completados.
3. Responsables dentro del tipo y estado seleccionados.
4. Acuerdos individuales dentro de cada responsable.

No se mostrará una pregunta como `¿Qué quieres revisar?`. Las tres opciones de tipo aparecerán directamente después del encabezado del Panel de acuerdos.

## Arquitectura de información

### Selector primario de tipo

El panel mostrará tres controles visibles y mutuamente excluyentes:

- Minutas;
- Compromisos;
- Proyectos.

Cada control incluirá su conteo total real dentro del alcance vigente de filtros y permisos. El tipo activo tendrá un estado visual inequívoco y `aria-current` o semántica equivalente.

La apertura predeterminada será `Minutas` cuando la URL no contenga un tipo válido. Los enlaces existentes que ya envían `tab=MINUTA`, `tab=COMPROMISO` o `tab=PROYECTO` conservarán su intención y abrirán el tipo correspondiente.

### Navegación secundaria por estado

Debajo del selector de tipo aparecerán los cinco estados existentes:

- Vencidos;
- Activos;
- Por aprobar;
- Prórrogas;
- Completados.

Todos los conteos se calcularán únicamente sobre el tipo seleccionado. Por ejemplo, si está activo `Minutas`, `Vencidos` será el número de Minutas vencidas y la lista contendrá solo Minutas vencidas.

La selección de estado conservará el tipo en la URL. Cambiar de tipo conservará el estado cuando ese estado sea válido, para permitir comparar categorías sin perder el contexto. Si la URL contiene valores inválidos, la vista normalizará a `Minutas` y `Vencidos`.

### Lista por persona

La agrupación por persona se conserva, pero se construirá después de aplicar tipo y estado. Cada fila mostrará:

- nombre o `Sin asignar`;
- número de elementos del tipo y estado activos;
- fecha relevante más próxima o más antigua según el estado;
- acceso a los acuerdos individuales.

Los textos serán específicos del contexto: `4 minutas vencidas`, `3 compromisos activos` o `2 proyectos completados`. No se utilizará el término genérico `acuerdos` cuando el tipo ya esté seleccionado.

### Lista completa

La opción para abrir todos los elementos permanecerá disponible, pero solo incluirá los del tipo y estado seleccionados. Cada registro conservará título, responsable, avance, estado operativo, área y fecha límite existentes.

### Filtros y auditoría

`Filtros y auditoría` conservará el filtro por colaborador y el estatus técnico. El selector técnico de tipo se retirará o quedará sincronizado con el selector primario para evitar dos controles que puedan contradecirse.

Los accesos a desfases e histórico seguirán disponibles. Cuando uno de esos alcances se active, respetará el tipo seleccionado y no volverá a mezclar categorías.

## Comportamiento y datos

La vista reutilizará `SeguimientoItem.tipo`, `visual_bucket` y `_item_en_estado_panel`. No se crearán modelos, migraciones, API ni estados persistidos.

El orden de procesamiento será:

1. aplicar permisos y filtros de alcance existentes;
2. calcular el estado visual de cada elemento;
3. resolver el tipo activo;
4. obtener los conteos de estados para ese tipo;
5. filtrar por el estado activo;
6. agrupar el resultado por responsable;
7. renderizar la plantilla.

Los conteos de las tres categorías se calcularán sobre el mismo alcance base para que el selector primario sea coherente. Los indicadores del encabezado, si se conservan, también serán específicos del tipo activo; no repetirán totales globales mezclados.

## URL y compatibilidad

Se preservará la ruta `/seguimiento/panel/` y los parámetros actuales compatibles:

- `tab=MINUTA|COMPROMISO|PROYECTO` para el tipo;
- `estado=vencidos|activos|revision|prorrogas|completados` para el estado;
- filtros de colaborador, estatus técnico, bucket o auditoría cuando correspondan.

No se crearán rutas nuevas. Los enlaces del menú superior y lateral continuarán usando `core/navigation.py` como fuente única.

## Estados de interfaz

### Vacío

Si no existen elementos para la combinación activa, la pantalla mostrará un mensaje específico, por ejemplo `No hay Minutas vencidas`, y mantendrá visibles los selectores de tipo y estado para cambiar de contexto.

### Error

La reorganización es renderizada por el servidor y no añade solicitudes asíncronas. Los errores de filtros inválidos se resolverán con valores predeterminados seguros, sin excepción visible ni pérdida de acceso al panel.

### Carga e interacción

No se añadirá un cargador artificial ni una SPA. Cada cambio de tipo o estado será un enlace normal que conserva parámetros compatibles. El foco, hover y estado activo respetarán WCAG AA y no dependerán solo del color.

## Responsive y accesibilidad

- En escritorio, los tres tipos se mostrarán en una fila clara.
- En móvil, se apilarán o permitirán desplazamiento horizontal sin recortar etiquetas ni conteos.
- Cada control tendrá un objetivo táctil mínimo de 44 px.
- El tipo y el estado activos tendrán texto, contraste y semántica accesible.
- Los acordeones por persona seguirán operables con teclado.
- La reorganización no añadirá animaciones necesarias para comprender el flujo.
- Se respetará `prefers-reduced-motion` para cualquier transición existente.

## Archivos previstos

- `seguimiento/views.py`: resolver primero el tipo, calcular estados dependientes del tipo y agrupar el resultado filtrado.
- `seguimiento/templates/seguimiento/panel_dg.html`: reemplazar la jerarquía global por selector de tipo y estados contextuales.
- `static/css/template_modules/seguimiento-templates-seguimiento-panel-dg.css`: jerarquía visual, estados activos y responsive.
- `seguimiento/tests.py`: cobertura de conteos, URLs, separación y agrupación.
- `static/erp-sw.js` o el service worker aplicable: solo si la inspección confirma que esta superficie o sus assets quedan bajo caché controlado.

No se prevén cambios en modelos, migraciones, permisos, autenticación, datos operativos ni navegación global.

## Contratos que deben preservarse

- Solo Dirección General y revisores globales autorizados acceden al panel.
- Los permisos y alcances de `SeguimientoItem` no cambian.
- Minutas, Compromisos y Proyectos conservan sus significados actuales.
- Los estados visuales y operativos existentes no se reinterpretan.
- Crear acuerdo, revisar detalle, aprobar, gestionar prórrogas y consultar auditoría conservan rutas y reglas actuales.
- El estado `Listo para cerrar` continúa separado del cierre auditado real.
- Los filtros y conteos se derivan del mismo conjunto autorizado; no se alteran registros.

## Criterios de aceptación

1. El primer nivel visible del panel contiene Minutas, Compromisos y Proyectos.
2. No aparece la frase `¿Qué quieres revisar?`.
3. No se muestran conteos principales de Vencidos o Activos que mezclen los tres tipos.
4. Al seleccionar Minutas, todos los conteos y registros visibles pertenecen solo a Minutas.
5. Al seleccionar Compromisos, todos los conteos y registros visibles pertenecen solo a Compromisos.
6. Al seleccionar Proyectos, todos los conteos y registros visibles pertenecen solo a Proyectos.
7. Cambiar de estado conserva el tipo seleccionado en la URL.
8. Cambiar de tipo conserva un estado válido y recalcula sus conteos.
9. La agrupación por persona y la lista completa no mezclan tipos.
10. Los enlaces existentes con `tab=` continúan abriendo el tipo correcto.
11. El estado vacío nombra el tipo y estado activos.
12. No se crean modelos, migraciones, rutas ni contratos de API.
13. La vista funciona con teclado, foco visible y contraste WCAG AA.
14. La pantalla se valida en escritorio y en móvil realista, con consola y solicitudes sin errores.

## Pruebas previstas

- Mezcla controlada de Minutas, Compromisos y Proyectos en todos los estados.
- Conteos de estado diferentes entre tipos para detectar sumas cruzadas.
- Responsable con elementos de más de un tipo: cada selección muestra solo el tipo activo.
- Elementos sin responsable dentro de cada categoría.
- URLs con `tab` y `estado` válidos e inválidos.
- Filtros de colaborador y estatus técnico combinados con el tipo primario.
- Alcances de desfases e histórico sin mezcla de tipos.
- Estado vacío por cada tipo.
- Acceso autorizado y rechazo del usuario sin permiso.
- Navegador en escritorio y móvil, incluyendo foco, consola y red.

## Fuera de alcance

- Cambiar cómo se crean, asignan, aprueban o cierran acuerdos.
- Modificar estados persistidos o reglas de vencimiento.
- Corregir o reclasificar datos existentes.
- Cambiar permisos, roles o navegación global.
- Introducir un framework JavaScript o una API nueva.
- Rediseñar el detalle individual del acuerdo.
