# Mi trabajo de colaboradores separado por tipo y estado

**Fecha:** 2026-09-28
**Estado:** Diseño aprobado por Mauricio
**Alcance:** Bandeja personal de seguimiento para todos los colaboradores

## Problema

La ruta personal `Mi trabajo` puede abrir una vista general que mezcla minutas,
proyectos y compromisos. Además, cuando la URL no incluye un estado válido, la
vista selecciona `Vencidos` si existe al menos un elemento atrasado. Para personas
con una cartera histórica grande, como Yesenia, esto provoca que la primera
pantalla sea una lista extensa de vencidos y que la relación entre el tipo elegido,
las cifras y los elementos visibles no sea suficientemente clara.

El panel de Dirección General ya estableció el patrón correcto: primero se elige
el tipo de acuerdo y después se consultan exclusivamente los estados y elementos
de ese tipo. La bandeja personal debe aplicar la misma jerarquía sin ampliar los
permisos del colaborador.

## Resultado aprobado

Todos los colaboradores entrarán a una bandeja con esta jerarquía:

1. Tipo: `Minutas`, `Proyectos` o `Compromisos`.
2. Estado del tipo seleccionado: `Vencidos`, `Activos`, `En revisión` o
   `Finalizados`.
3. Lista de elementos asignados que coincidan con ambos filtros.

La apertura predeterminada será siempre `Minutas` + `Activos`. La existencia de
elementos vencidos no cambiará automáticamente ese estado inicial. Si no hay
minutas activas, la vista mostrará el estado vacío de `Activos` y permitirá que la
persona elija otro estado o tipo; nunca saltará silenciosamente a `Vencidos`.

## Experiencia de usuario

### Entrada

- Para un colaborador, `/seguimiento/` renderizará directamente la vista de
  minutas activas, conservando la compatibilidad con la vista previa de DG.
- Los accesos de navegación existentes a Minutas, Proyectos y Compromisos se
  conservarán.
- Dirección General conservará su redirección al panel de equipo; esta tarea no
  modifica ese flujo.

### Selector de tipo

- La parte superior mostrará tres tarjetas claras: Minutas, Proyectos y
  Compromisos.
- Cada tarjeta mostrará el total asignado de su tipo, considerando todos sus
  estados.
- La tarjeta seleccionada tendrá texto, borde e indicador visual accesible; la
  selección no dependerá únicamente del color.
- Al cambiar de tipo, la pantalla abrirá `Activos` para ese nuevo tipo.

### Selector de estado

- Debajo del tipo seleccionado se mostrarán cuatro estados con sus cantidades.
- Las cantidades se calcularán únicamente con elementos del tipo seleccionado.
- Elegir un estado conservará el tipo actual en la URL y en el encabezado.
- `Vencidos` seguirá disponible y visible, pero nunca será la apertura automática.

### Lista

- La lista contendrá solo elementos que coincidan con el tipo y estado activos.
- Los textos de encabezado y estados vacíos usarán el nombre del tipo elegido.
- Las acciones existentes (`Iniciar`, `Continuar`, `Ver seguimiento` o
  `Consultar`) conservarán su comportamiento y llevarán al detalle actual.
- Las aprobaciones de pasos asignadas a la persona permanecerán en su sección
  independiente.

## Reglas de filtrado

La fuente de datos seguirá siendo `_items_del_usuario(request.user)`. Por tanto,
la corrección no cambia quién puede ver un acuerdo ni altera responsables,
participantes, permisos o datos operativos.

El orden de cálculo será:

1. Obtener exclusivamente los elementos visibles para el usuario autenticado.
2. Calcular la presentación y el estado visual existentes de cada elemento.
3. Agrupar los elementos por tipo para obtener los totales de las tres tarjetas.
4. Filtrar por el tipo activo.
5. Agrupar ese subconjunto por estado.
6. Mostrar exclusivamente el estado activo, cuyo valor predeterminado será
   `activos`.

Los estados conservarán las reglas actuales:

- `Vencidos`: abiertos, fuera de plazo y no enviados a revisión.
- `Activos`: abiertos, dentro de plazo y no enviados a revisión.
- `En revisión`: abiertos con estatus de revisión.
- `Finalizados`: acuerdos cerrados.

## URL y navegación

- Se mantendrán las rutas existentes:
  - `/seguimiento/minutas/`
  - `/seguimiento/proyectos/`
  - `/seguimiento/compromisos/`
- El estado continuará expresándose como `?estado=<valor>`.
- Los enlaces de tipo apuntarán al estado `activos` para que el cambio de tipo no
  herede accidentalmente una cartera vencida.
- Los enlaces de estado conservarán la ruta del tipo seleccionado.
- Parámetros desconocidos no causarán error: se normalizarán a `activos`.

## Componentes y archivos previstos

- `seguimiento/views.py`: selección canónica del tipo y estado, conteos por tipo y
  estado, y contexto de renderizado.
- `seguimiento/templates/seguimiento/mi_seguimiento.html`: tarjetas de tipo,
  resumen seleccionado, navegación de estados y textos específicos.
- `static/css/template_modules/seguimiento-templates-seguimiento-mi-seguimiento.css`:
  jerarquía visual, estados seleccionados, adaptación móvil, foco y reducción de
  movimiento.
- `seguimiento/tests.py`: pruebas de aislamiento por tipo, estado inicial activo,
  navegación y permisos.
- Archivos de versión del service worker y sus expectativas de prueba, porque la
  tarea modifica una pantalla y un estático visibles del ERP.

No se prevén cambios de modelos, migraciones, API, permisos, autenticación ni
datos de producción.

## Casos límite

- Usuario sin elementos: verá Minutas + Activos con conteos en cero y un estado
  vacío útil.
- Usuario con vencidos pero sin activos: seguirá entrando a Activos; Vencidos
  mostrará su cantidad y podrá elegirse de forma explícita.
- Usuario con elementos de un solo tipo: las otras tarjetas permanecerán visibles
  con cero para mantener una navegación uniforme entre colaboradores.
- Estado inválido o ausente: se normaliza a Activos.
- Usuario DG: conserva el panel general y sus parámetros actuales.
- Vista previa como colaborador: aplica exactamente las mismas reglas que el
  usuario representado, en modo de solo lectura.

## Accesibilidad y respuesta visual

- Los selectores serán enlaces semánticos con `aria-current` en tipo y estado
  activos.
- Habrá foco visible para teclado.
- La selección incluirá texto y estructura, no solo color.
- En móvil, las tarjetas de tipo se apilarán y los estados podrán desplazarse sin
  causar desbordamiento horizontal del documento.
- Se respetará `prefers-reduced-motion`.

## Validación

### Pruebas automatizadas

- La ruta general de colaborador abre Minutas activas.
- Minutas, Proyectos y Compromisos calculan conteos independientes.
- La lista no incluye elementos de otro tipo ni de otro estado.
- Un usuario con vencidos y cero activos sigue abriendo Activos.
- Cambiar de tipo abre Activos y cambiar de estado conserva el tipo.
- Un colaborador no obtiene acceso a acuerdos ajenos.
- Dirección General conserva su redirección al panel del equipo.
- El service worker y sus expectativas comparten la misma versión.

### Verificación visual

- Escritorio y móvil con datos representativos.
- Vista previa real de al menos un colaborador con cartera amplia.
- Navegación entre los tres tipos y los cuatro estados.
- Ausencia de errores de consola y solicitudes fallidas relevantes.
- Verificación autenticada en producción después del despliegue.

## Criterios de aceptación

1. Ningún colaborador abre automáticamente en Vencidos.
2. La pantalla inicial es Minutas + Activos para todos los colaboradores.
3. Los conteos de estado pertenecen solo al tipo seleccionado.
4. La lista visible pertenece solo al tipo y estado seleccionados.
5. Cambiar de tipo nunca conserva Vencidos de forma implícita.
6. El comportamiento aplica tanto al usuario directo como a la vista previa de
   colaboradores.
7. No se modifican asignaciones, permisos ni datos operativos.
8. La solución queda validada localmente, en CI y en la pantalla autenticada de
   producción.

## Fuera de alcance

- Reclasificar, cerrar o prorrogar acuerdos existentes.
- Corregir fechas históricas o cantidades de la cartera vencida.
- Cambiar la lógica de asignación o visibilidad de colaboradores.
- Rediseñar el detalle de cada acuerdo.
- Crear nuevos estados o tipos de seguimiento.
