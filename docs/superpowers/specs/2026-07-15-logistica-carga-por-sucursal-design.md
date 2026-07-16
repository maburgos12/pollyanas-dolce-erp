# Logística: carga por sucursal con contexto operativo

## Objetivo

Simplificar la revisión física de carga en el PWA. El repartidor debe confirmar una sucursal completa por vez, con las cantidades `Enviado` de Point precargadas y editables, en lugar de confirmar producto por producto.

El cambio también debe centralizar la autorización y consistencia de la ruta en un `contexto_operativo` versionado para impedir que usuarios, unidades, tramos, productos o reintentos offline obsoletos se mezclen.

## Alcance

- PWA de Logística para el chofer titular de la ruta.
- API y servicios de checklist de carga.
- Tramos delimitados por paradas CEDIS.
- Captura masiva y atómica por sucursal.
- Registro y revisión posterior de diferencias por el jefe inmediato.
- Bloqueo de Planeación al día siguiente cuando existan diferencias vencidas sin clasificar.
- Notificaciones, auditoría, pruebas y versionado del service worker.

No se modifica el contrato operativo del acompañante: puede estar planeado y visible, pero no opera la ruta ni el checklist.

## Contexto operativo canónico

El backend construirá el contexto; el cliente no decidirá qué ruta, unidad, tramo o acciones están permitidas.

```text
contexto_operativo
├── ruta_id
├── chofer_autorizado_id
├── unidad_id
├── tramo_id
├── parada_cedis_origen_id
├── version_checklist
├── sucursales_permitidas
├── productos_permitidos
└── acciones_permitidas
```

### Invariantes

1. `chofer_autorizado_id` corresponde al repartidor titular, nunca al acompañante.
2. `unidad_id` procede de la ruta activa; el perfil del usuario no puede sustituirla.
3. `tramo_id` identifica de forma estable el intervalo entre la salida de CEDIS y el siguiente CEDIS.
4. `version_checklist` cambia cuando Point o el ERP modifica las líneas operativas del tramo.
5. Cada sucursal y producto aceptado debe pertenecer al contexto vigente.
6. Toda mutación incluye un identificador idempotente.
7. El servidor valida de nuevo el contexto dentro de la transacción de guardado.

## Flujo del repartidor

### 1. Resumen del tramo

La pantalla muestra únicamente las sucursales del tramo operativo actual. Cada botón incluye nombre, número de productos y estado:

- `Pendiente`
- `Guardada`
- `Guardada con diferencias`

`Salir a ruta` permanece deshabilitado hasta que todas las sucursales estén guardadas.

### 2. Captura de una sucursal

Al seleccionar una sucursal:

- Los productos aparecen en orden alfabético.
- Un buscador compacto filtra por nombre o código.
- Cada fila muestra nombre, código cuando exista, cantidad `Enviado`, unidad y campo de cantidad cargada.
- La cantidad cargada inicia con el valor `Enviado` de Point.
- Todos los campos permanecen editables mientras el tramo siga abierto en CEDIS.
- Una cantidad modificada se resalta visualmente sin saturar la lista.
- `Guardar sucursal` permanece fijo al pie de la pantalla en celular.

### 3. Popup de diferencias

Al pulsar `Guardar sucursal`:

- Si ninguna cantidad cambió, se guarda directamente.
- Si existen cambios, se abre un único modal sobre la captura; no se navega a otra pantalla.
- El modal contiene únicamente los productos modificados.
- Cada diferencia muestra `Enviado → Cargado`, exige un motivo y admite nota opcional.
- Cerrar el modal, incluso tocando `Cancelar`, conserva todas las cantidades capturadas y devuelve el foco a `Guardar sucursal`.
- Confirmar envía la sucursal completa.

Motivos iniciales:

- Faltante físico.
- Producto dañado.
- Error en Point.
- Cambio autorizado.
- Otro.

### 4. Guardado y cierre del tramo

El guardado por sucursal es atómico: se aplican todas sus líneas o ninguna.

Después de guardar, el PWA regresa al resumen del tramo. Una sucursal puede abrirse y editarse nuevamente mientras no se haya ejecutado `Salir a ruta`.

Al pulsar `Salir a ruta`:

- Todas las sucursales deben estar guardadas con la versión vigente.
- El tramo queda cerrado para edición de carga.
- Durante el recorrido solo se confirman entregas; no se reescribe lo que salió de CEDIS.

## Contrato de captura masiva

La mutación por sucursal incluirá:

```json
{
  "ruta_id": 64,
  "tramo_id": "cedis-271:ordenes-4-5",
  "sucursal_id": 7,
  "version_checklist": "sha256:...",
  "client_event_id": "uuid",
  "lineas": [
    {
      "linea_id": 20554,
      "source_hash": "25ef52b3be93c594d2a4",
      "cantidad_cargada": "2.000",
      "motivo_diferencia": "",
      "notas": ""
    }
  ]
}
```

El backend responderá con el contexto y resumen actualizados. Los errores de consistencia usarán códigos explícitos:

- `contexto_obsoleto`
- `tramo_cambiado`
- `checklist_actualizado`
- `sucursal_no_permitida`
- `producto_no_vigente`
- `motivo_diferencia_requerido`

Cuando el contexto cambie, la respuesta incluirá los productos afectados para que el PWA pueda recargar sin un mensaje genérico.

## Offline e idempotencia

- La captura offline conserva ruta, tramo, sucursal y versión del checklist.
- Solo se reintenta si el contexto sigue vigente.
- Al cambiar de tramo se eliminan capturas encoladas del tramo anterior.
- Un `client_event_id` repetido devuelve el resultado previo y no duplica cambios ni notificaciones.
- Una captura rechazada nunca se aplica parcialmente.

## Diferencias y revisión del jefe

Las diferencias no bloquean la ruta actual.

Al guardar una sucursal con diferencias se crea un caso auditable por producto y se asigna a:

1. `jefe_directo` del empleado vinculado al chofer en RRHH.
2. Responsable del departamento que tiene a cargo Logística; actualmente Ventas.
3. Fallback de Logística/DG cuando no exista una relación válida.

El jefe dispone de tres acciones:

- `Validar como real`.
- `Marcar como incorrecta`.
- `Solicitar aclaración`.

Cada decisión conserva usuario, fecha, estado anterior, comentario y evidencia de ruta. La revisión no cambia automáticamente las cantidades históricas.

## Deuda obligatoria al día siguiente

La ruta con diferencias puede continuar y finalizar el mismo día.

Al entrar a Planeación en una fecha posterior, el jefe responsable debe atender primero las diferencias pendientes de días anteriores. Mientras existan casos vencidos sin clasificar:

- Puede consultar la bandeja obligatoria.
- No puede crear ni modificar la planeación nueva.
- El bloqueo muestra las diferencias que faltan y enlaces directos para resolverlas.

Los casos en `Solicitar aclaración` cuentan como atendidos por el jefe y no mantienen el bloqueo de Planeación; quedan en seguimiento separado con el repartidor. Así, el jefe no puede omitir la revisión, pero una aclaración pendiente del operador tampoco paraliza toda la planeación.

## Diseño móvil y marca

La implementación seguirá `PRODUCT.md`, `DESIGN_STACK.md` y los patrones vigentes de la App Operativa.

- Paleta vino, dorado y fondos claros de Pollyana's Dolce.
- Títulos editoriales y controles operativos legibles.
- Una fila compacta por producto; no una tarjeta grande por producto.
- Cantidad y unidad alineadas a la derecha.
- Buscador y encabezado compactos.
- Acción principal fija al pie respetando safe areas.
- Modal de diferencias con foco atrapado, cierre por Escape y retorno de foco.
- Contraste, objetivos táctiles y mensajes accesibles.

## Modelo de estados

### Sucursal del tramo

```text
PENDIENTE → GUARDADA
PENDIENTE → GUARDADA_CON_DIFERENCIAS
GUARDADA* → PENDIENTE_EDITADA → GUARDADA*
GUARDADA* → CERRADA_AL_SALIR
```

### Revisión de diferencia

```text
PENDIENTE_JEFE → VALIDADA_REAL
PENDIENTE_JEFE → MARCADA_INCORRECTA
PENDIENTE_JEFE → ACLARACION_SOLICITADA
ACLARACION_SOLICITADA → VALIDADA_REAL | MARCADA_INCORRECTA
```

## Pruebas de aceptación

1. El chofer titular recibe contexto; el acompañante no puede operar.
2. La unidad procede de la ruta activa.
3. Solo aparecen las sucursales y productos del tramo actual.
4. Productos ordenados alfabéticamente y filtrables por buscador.
5. Cantidades `Enviado` precargadas y editables.
6. Guardado completo sin diferencias.
7. Popup único para múltiples diferencias con motivo obligatorio por producto.
8. Fallo en cualquier línea revierte toda la sucursal.
9. Cambio de Point produce `checklist_actualizado` con detalle, sin escritura parcial.
10. Reintento idempotente no duplica registros.
11. Mutación offline vieja no cruza de tramo.
12. Sucursal editable antes de `Salir a ruta` y bloqueada después.
13. Salida habilitada solo con todas las sucursales guardadas.
14. Diferencias asignadas al jefe directo/Ventas y fallback Logística/DG.
15. Las tres decisiones del jefe quedan auditadas.
16. La ruta actual nunca se bloquea por diferencias.
17. Planeación posterior se bloquea por pendientes vencidos y se habilita al clasificarlos.
18. PWA validada en celular, consola y Network; service worker versionado y desplegado.

## Entrega y validación

La implementación se hará en una rama/worktree limpio y deberá completar:

1. Pruebas de servicios y API con PostgreSQL.
2. Pruebas del contrato JavaScript del PWA.
3. Validación local autenticada en navegador móvil.
4. PR, CI, merge y deploy mediante `scripts/deploy_web_safe.sh`.
5. `collectstatic` y actualización del service worker.
6. Validación en producción con un usuario/recorrido real antes de declarar terminado.
