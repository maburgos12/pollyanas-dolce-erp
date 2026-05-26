# Pollyana Ops UI

Sistema visual oficial para el ERP de Pollyana's Dolce.

Este documento es obligatorio para cualquier adicion de grupo, modulo, seccion,
PWA, reporte, formulario, dashboard, correo HTML o flujo operativo visible.

## Posicion

El ERP debe sentirse como una herramienta operativa hecha para Pollyana's Dolce:
calida, sobria, compacta, auditable y especifica para una cadena de pastelerias.
No debe parecer una landing page, un SaaS generico ni una pantalla generada por IA.

## Principios

1. Operacion primero. La primera vista debe mostrar que hacer ahora.
2. Marca contenida. El vino identifica, el dorado acentua, ningun color grita.
3. Densidad ordenada. Mas informacion visible, menos decoracion.
4. Una sola voz. Navegacion, botones, tablas, formularios, badges y modales deben compartir reglas.
5. Evidencia visible. Fechas, fuentes de datos, usuario afectado y estado operativo deben ser claros.
6. Auditoria sin ruido. Lo avanzado o ejecutivo puede existir, pero no debe tapar el trabajo principal.

## Tokens

Todo color, fuente, radio, sombra y spacing reutilizable debe vivir como token CSS.
Usar los tokens de `static/css/pollyana_ops_ui.css`.

Tokens primarios:

- `--pd-wine`: identidad principal.
- `--pd-gold`: acento escaso.
- `--pd-ink`: texto principal.
- `--pd-muted`: texto secundario.
- `--pd-paper`: fondo general.
- `--pd-surface`: paneles.
- `--pd-border`: bordes.
- `--pd-success`, `--pd-warning`, `--pd-danger`, `--pd-info`: estados.

Regla: no agregar hex, RGB, HSL u OKLCH nuevos en templates. Si falta un color,
se agrega token con nombre semantico.

## Tipografia

Stack oficial:

- Cuerpo: `Nunito`.
- Numeros operativos: `Nunito` con ancho tabular.
- Display moderado: `Playfair Display`.

Playfair se usa solo para marca, titulos de pagina, KPIs principales o encabezados
de seccion. No usar Playfair para tablas, formularios, botones ni texto denso.
No usar `Inter`, `Roboto`, `Open Sans` ni fuentes nuevas en canvas o modulos aislados.

Regla numerica obligatoria:

```css
font-family: var(--pd-font-number);
font-variant-numeric: tabular-nums;
font-feature-settings: var(--pd-numeric-features);
letter-spacing: 0;
```

Usar esta regla en dinero, porcentajes, piezas, tickets, IDs visibles, comparativos
y ejes/tooltips de graficas. La alineacion debe ser a la derecha en tablas y
matrices auditables; en KPIs puede mantenerse la alineacion del componente.
En templates Django, los importes y cantidades visibles deben aplicar separador
de miles con `intcomma` despues de `floatformat`, por ejemplo
`${{ valor|floatformat:2|intcomma }}`.
La regla se audita con `python3 scripts/audit_numeric_format.py`; no corregir
variables dentro de `style`, `value`, `data-*`, `<script>` o calculos tecnicos,
porque esas salidas deben seguir siendo numeros sin separador para el navegador.

## Estructura Por Tipo De Pantalla

### Modulo operativo

Orden recomendado:

1. Encabezado compacto con titulo, contexto y accion principal.
2. Filtros esenciales.
3. Formulario, tabla o cola de trabajo.
4. Resumen KPI breve.
5. Detalle avanzado en `details` o panel colapsable.

### Dashboard ejecutivo

Orden recomendado:

1. Corte visible y fuente de datos.
2. KPIs principales.
3. Graficas o comparativas clave.
4. Tabla auditable debajo o colapsable.

Reglas para graficas:

- Toda grafica debe vivir en un panel con titulo, corte y fuente visible.
- Toda grafica debe tener tabla/lista auditable cerca, visible o en `details`.
- Si Chart.js o el canvas no cargan, la pantalla debe seguir mostrando lectura textual o tabla.
- Usar colores del sistema, no paletas nuevas por dashboard.
- No mostrar metricas sin corte, fuente o estado de cobertura.

Reglas para tablas de dashboard:

- Encabezados sticky cuando la tabla sea larga.
- Numeros, dinero, porcentajes y piezas alineados a la derecha con numeros tabulares.
- Scroll horizontal solo dentro del contenedor de tabla.
- Estados con badges consistentes: `success`, `warning`, `danger`, `neutral`.
- La tabla debe responder que dato audita el KPI o grafica cercano.

### Administracion o configuracion

Orden recomendado:

1. Tabla/listado primero.
2. Filtros compactos.
3. Accion primaria `Nuevo`.
4. Formulario o modal dedicado.

### PWA movil

Orden recomendado:

1. Estado de sesion.
2. Acciones grandes y claras.
3. Flujo por pasos.
4. Confirmacion con folio o evidencia.

## Componentes

Usar componentes compartidos antes de crear HTML nuevo:

- `templates/components/section_header.html`
- `templates/components/kpi_card.html`
- `templates/components/status_badge.html`
- `templates/components/empty_state.html`
- clases `.pd-page-gutters`, `.pd-page-head`, `.pd-work-panel`, `.pd-kpi-strip`, `.pd-action-row`, `.pd-filter-row`

Si un modulo necesita un patron que se repetira, crear componente compartido.

Regla de marco global: el `main-content` del ERP debe mantener separacion
lateral contra la barra izquierda y contra el borde derecho de la ventana. No
compensar esto con margenes negativos ni contenedores full-bleed, salvo print,
PWA dedicada o una excepcion documentada.

Regla de proporcion Hallmark: no combinar `main-content`, `.pd-page-gutters`,
`.container` y padding propio del modulo para crear doble o triple marco. Cada
pantalla debe tener un solo marco lateral, un ancho maximo acorde a su densidad
operativa y cards sin contenedores decorativos anidados. Si una pantalla se ve
centrada con demasiado blanco en monitores amplios, aumentar el ancho util del
modulo o compactar la composicion antes de agregar mas cards.

Regla de prioridad Hallmark 2026-05: dashboards, reportes densos y centros de
control usan ancho util de hasta 1720 px mediante `.module-shell` o `.container`.
No crear shells locales de 1200-1320 px salvo formularios estrechos justificados.
Los paneles de trabajo usan radio 8-10 px, fondo plano y sombra baja; las
pastillas pueden seguir siendo redondas, pero las cards y KPIs no deben adoptar
radios de 16-24 px ni gradientes decorativos.

Regla de foco: todos los campos editables (`input`, `select`, `textarea`,
`.form-control`, `.form-select`, `.input-field`) usan borde y halo dorado en
focus. No usar foco vino, azul nativo ni anillos dobles por modulo.

Regla de listas buscables: todo selector de productos, recetas, insumos,
sucursales, proveedores, clientes, empleados, responsables, unidades o listas
largas debe permitir teclear para filtrar. El `select` original sigue siendo la
fuente de verdad para el POST; la capa visual solo facilita busqueda y seleccion.
No permitir texto libre si el backend espera un id u opcion cerrada: el valor
es valido solo si coincide con una opcion real. Los selects cortos de estatus o
booleanos pueden permanecer nativos, salvo que el modulo marque
`data-searchable-select="true"`.

Regla de encabezado superior: pantallas operativas, dashboards internos y
modulos nuevos deben envolver el contenido en `.pd-page-gutters` e iniciar con `.pd-page-head`, `.pd-eyebrow`,
`.pd-page-title`, `.pd-page-copy` y `.pd-action-row`. No usar heroes grandes,
fondos oscuros, gradientes, bloques full-width tipo landing ni encabezados por
modulo que compitan con el shell del ERP. El contenido no debe quedar pegado a
la barra lateral izquierda ni al borde derecho de la ventana.

## Reglas Obligatorias

1. No agregar `style=""` en templates nuevos, salvo correos, print/PDF o excepcion documentada.
2. No agregar bloques `<style>` en templates nuevos. Usar CSS compartido o CSS del modulo.
3. No usar cards dentro de cards para decorar.
4. No usar emojis como iconos funcionales.
5. No usar gradientes grandes, blobs, orbes, glassmorphism o sombras glow.
6. No usar `transition: all`.
7. No ocultar problemas de layout con `overflow-x: hidden`; usar `clip` en shell y scroll visible en tablas.
8. Todo boton tiene estados default, hover, focus-visible, active, disabled y loading si aplica.
9. Todo input mantiene el mismo border-width entre estados.
10. Numeros monetarios, porcentajes y cantidades usan `--pd-font-number` con numeros tabulares.
11. Botones, tabs y links de navegacion no se parten en dos lineas.
12. Tablas y badges no parten palabras. Encabezados, fechas, codigos, estatus,
    importes y acciones usan `nowrap`; columnas largas solo pueden envolver por
    espacios con `.cell-wrap`, `.cell-long` o `data-cell-wrap="true"`.
13. Selects/listas de entidades o listas largas deben ser buscables por teclado y bloquear texto libre invalido.
14. Validar mobile en 320, 375, 414 y 768 px cuando toque UI visible.

## Navegacion

Los grupos nuevos se agregan en `core/navigation.py` y deben cumplir:

- Nombre de grupo corto y operativo.
- Submodulos con verbos o sustantivos claros.
- No duplicar rutas visibles en dos grupos sin razon operativa.
- Si el grupo no esta listo para usuarios, no aparece en sidebar.
- Permisos y visibilidad se validan con el usuario real afectado.

## Modulos Nuevos

Antes de implementar un modulo nuevo, definir:

- Usuario principal.
- Trabajo principal que resuelve.
- Accion primaria.
- Tabla o entidad principal.
- Estados vacio, error, loading y sin permisos.
- Fuente de datos visible si muestra metricas.
- Ruta y grupo de navegacion.

## Anti Patrones Bloqueados

- Hero decorativo de pantalla completa.
- Tres cards iguales para explicar funciones.
- Card dentro de card.
- Gradiente morado/azul o texto con gradiente.
- Emojis como iconos.
- Colores inline.
- Botones con labels largos que envuelven.
- Metricas inventadas o sin fuente.
- Graficas sin tabla o fallback auditable.

## Definicion De Terminado

Un cambio visual no esta terminado hasta que:

- pasa `python manage.py check`;
- si agrega estaticos, se considera `collectstatic --noinput` en deploy;
- se revisa en navegador real si afecta UI;
- no agrega nueva deuda de estilo segun `scripts/audit_ui_style.py`;
- no deja numeros visibles sin separador segun `scripts/audit_numeric_format.py`;
- se valida el flujo final donde el usuario lo usa.
