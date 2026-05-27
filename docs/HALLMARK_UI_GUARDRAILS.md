# Hallmark UI Guardrails

Estas reglas son obligatorias para vistas nuevas o existentes del ERP.

## Capa obligatoria

- Toda vista que extienda `templates/base.html` carga `static/css/hallmark_guardrails.css` despues del CSS local del modulo.
- El contenido operativo vive bajo `main.main-content[data-hallmark-scope="erp"]`.
- Ningun modulo debe depender de `max-width` pequeno con `margin: 0 auto` para pantallas de escritorio grandes. El marco global usa el ancho operativo disponible y conserva gutters laterales.

## Reglas visuales

- Los shells principales (`.module-shell`, `.bonos-shell`, `.rrhh-page`, `.bi-shell`, `.costeo-shell`, etc.) deben ocupar el ancho operativo, con `max-width` global y alineacion consistente.
- Los grids de metricas, filtros, configuraciones y formularios deben usar columnas responsivas basadas en `auto-fit` o reglas globales equivalentes.
- Botones, chips, badges y estatus no deben cortar palabras ni salirse de su figura.
- Tablas pueden tener scroll horizontal interno cuando el numero de columnas lo exige, pero no deben generar scroll horizontal de pagina.
- Inputs, selects y textareas siempre deben tener `min-width: 0`, ancho maximo de su contenedor y altura minima consistente.

## Validacion minima

Antes de publicar cambios visuales:

- Revisar la vista en desktop ancho, tablet y movil.
- Confirmar `document.documentElement.scrollWidth - document.documentElement.clientWidth === 0`.
- Confirmar que `hallmark_guardrails.css` esta cargado.
- Revisar que botones y chips no tengan `scrollWidth > clientWidth`.
- Para tablas, validar que el scroll sea interno al `.table-responsive`, no de toda la pagina.
