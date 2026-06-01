# Hallmark UI Guardrails

Estas reglas son obligatorias para vistas nuevas o existentes del ERP.

## Capa obligatoria

- Toda vista que extienda `templates/base.html` carga `static/css/hallmark_guardrails.css` despues del CSS local del modulo.
- El contenido operativo vive bajo `main.main-content[data-hallmark-scope="erp"]`.
- Ningun modulo debe depender de `max-width` pequeno con `margin: 0 auto` para pantallas de escritorio grandes. El marco global usa el ancho operativo disponible y conserva gutters laterales.

## Reglas visuales

- Los shells principales (`.module-shell`, `.bonos-shell`, `.rrhh-page`, `.bi-shell`, `.costeo-shell`, etc.) deben ocupar el ancho operativo, con `max-width` global y alineacion consistente.
- Los grids de metricas, filtros, configuraciones y formularios deben usar columnas responsivas basadas en `auto-fit` o reglas globales equivalentes.
- Botones, chips, badges, KPI y estatus no deben cortar letras, numeros, palabras ni salirse de su figura.
- Los badges y estatus deben centrarse con `max-width: 100%`, `min-width: 0`, `white-space: normal` y texto envuelto dentro de la figura.
- Los numeros de KPI deben usar numeros tabulares y una regla de contencion; si el valor crece, se reduce o envuelve dentro del card, nunca se corta por el borde.
- Tablas pueden tener scroll horizontal interno cuando el numero de columnas lo exige, pero no deben generar scroll horizontal de pagina.
- Si una tabla analitica supera el primer plano operativo (aprox. 8 columnas de lectura primaria o 10+ columnas totales), debe dividirse en tabla principal y tabla secundaria, o en un detalle expandible. No debe depender de que el usuario lea toda la operacion con scroll horizontal.
- Las columnas de estado con frases largas deben ganar ancho o moverse a detalle secundario; no se permite cortar frases como `Faltante no explicado` por falta de columna.
- Inputs, selects y textareas siempre deben tener `min-width: 0`, ancho maximo de su contenedor y altura minima consistente.

## Reportes

- Los tabs del modulo `Reportes` deben usar `.module-tabs.report-tabs`; la regla global los renderiza como grid responsivo y evita una tira horizontal blanda.
- Los reportes analiticos deben separar metrica operativa principal (`venta`, `produccion`, `diferencia`, `merma`, `costo`, `estado`) del detalle secundario (`conversion`, `inventario`, equivalencias) cuando la tabla completa ya no cabe en el primer campo visual.

## Capital Humano

- Las vistas del grupo Capital Humano (`.rrhh-page`, `.ch-page`, `.indicadores-page`, `.org-page`, `.vacantes-page`, `.permisos-page`, `.assign-page`, `.loan-page`, `.monitor-page`) estan cubiertas por la hoja global.
- Los tabs de submodulos deben usar `.module-tabs.rrhh-tabs`; la regla global los renderiza como grid responsivo, no como una tira flexible que se aplasta o depende de scroll horizontal.
- Los tableros de permisos, horas extra, prestamos, indicadores, vacantes y organizacion deben usar las familias existentes (`.permisos-board`, `.ch-kanban`, `.loan-board`, `.indicadores-kpis`, `.vacantes-stats`, `.org-stats`) para heredar columnas responsivas.
- Los selectores operativos por area (`.rule-selector`, `.rule-tab`, `.area-grid`, `.area-chip`) deben permitir salto de linea interno; ningun titulo, conteo o importe debe quedar cortado por una tarjeta rigida.
- Los formularios de alta, edicion, importacion o detalle deben usar `.form-grid`, `.rrhh-form-grid`, `.indicador-form-grid`, `.vacantes-grid` o `.ch-fields`; no deben fijar anchos que obliguen scroll horizontal de pagina.
- Las tablas de Capital Humano deben vivir dentro de `.table-responsive` o un contenedor equivalente; si requieren muchas columnas, el scroll es interno a la tabla.
- Los estatus y chips (`.rrhh-status`, `.permiso-badge`, `.vacantes-badge`, `.org-badge`, `.org-chip`) siempre deben centrarse y conservar palabras completas dentro de la figura.

## Validacion minima

Antes de publicar cambios visuales:

- Revisar la vista en desktop ancho, tablet y movil.
- Confirmar `document.documentElement.scrollWidth - document.documentElement.clientWidth === 0`.
- Confirmar que `hallmark_guardrails.css` esta cargado.
- Revisar que botones y chips no tengan `scrollWidth > clientWidth`.
- Revisar que KPI, badges, estatus y celdas visibles no tengan `scrollWidth > clientWidth`.
- Para tablas, validar que el scroll sea interno al `.table-responsive`, no de toda la pagina.

## Gate obligatorio

- `python manage.py check` ejecuta el check `hallmark.E002` y falla si aparece una regresion visual nueva contra `docs/hallmark_ui_audit_baseline.json`.
- `python manage.py check_hallmark_ui` muestra el detalle de cada regresion nueva: tablas sin wrapper, tabs sin familia responsive, grids rigidos, anchos inline, `overflow-x:hidden` y `nowrap` local.
- La baseline solo representa deuda existente; no debe actualizarse para aceptar codigo nuevo salvo revision explicita del cambio visual.
- Para una vista nueva, usar clases cubiertas por guardrails desde el inicio: `.module-tabs.rrhh-tabs`, `.module-tabs.report-tabs`, `.table-responsive`, `.kpi-grid`, `.form-grid`, `.status-pill`, `.badge`, `.rule-selector`, `.rule-tab`, `.area-grid` y `.area-chip`.
