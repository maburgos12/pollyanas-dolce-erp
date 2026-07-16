# Tutorial único de carga por sucursal

## Objetivo

Explicar a los repartidores, una sola vez por cuenta, el nuevo flujo de carga por sucursal inmediatamente después de iniciar sesión en la PWA de Logística. El tutorial no debe convertirse en una pantalla diaria ni bloquear el trabajo operativo.

## Alcance

El cambio comprende:

- un tutorial móvil de cinco pasos dentro de una hoja que se desliza desde abajo;
- persistencia en servidor de la confirmación por cuenta de repartidor;
- activación exclusiva para repartidores existentes al momento del lanzamiento;
- integración posterior al inicio de sesión y a la carga del perfil;
- explicación de la liberación de ruta al completar todas las sucursales del tramo;
- pruebas funcionales, visuales, de accesibilidad, caché PWA y producción.

No cambia las reglas de carga, discrepancias, bitácora, unidad o liberación de ruta ya implementadas. Tampoco crea un centro permanente de anuncios.

## Experiencia aprobada

### Aparición

La hoja aparece después de autenticar al repartidor y cargar correctamente su perfil. La PWA sigue visible y desenfocada detrás para conservar el contexto.

La hoja ocupa aproximadamente 85 % de la altura disponible, respeta las áreas seguras del teléfono y se presenta desde la parte inferior. El tutorial usa los colores vino, crema y dorado, tipografía y controles existentes de Pollyana's Dolce.

### Contenido

1. **Revisa una sucursal a la vez.** Muestra Leyva y Las Glorias como ejemplo y explica el regreso al listado después de guardar.
2. **Todas las cantidades se pueden editar.** La cantidad enviada aparece por defecto; cualquier campo puede corregirse.
3. **Guarda toda la sucursal con un botón.** El usuario no confirma producto por producto.
4. **Explica solo lo que cambió.** Únicamente los productos modificados requieren motivo y se envían a revisión sin detener la ruta.
5. **Completa el tramo y sal a ruta.** Cuando todas las sucursales del tramo están guardadas se habilita `Salir a ruta`.

### Navegación

- Los pasos se recorren con gesto horizontal o botones `Atrás` y `Siguiente`.
- Los indicadores muestran el paso actual.
- `Ahora no`, cerrar la hoja o cerrar la aplicación no registra el tutorial como visto.
- Solo `Entendido, comenzar` guarda la confirmación.
- La ruta no inicia automáticamente al terminar el tutorial ni al guardar la última sucursal.

### Movimiento y accesibilidad

- La hoja entra desde abajo mediante `transform` y `opacity`.
- Los pasos cambian horizontalmente en aproximadamente 220 ms.
- Se evita animar propiedades de layout.
- Con `prefers-reduced-motion` los cambios son inmediatos.
- La hoja usa semántica de diálogo, atrapa el foco, permite Escape como equivalente a `Ahora no` y devuelve el foco al elemento previo.
- Los botones tienen nombres accesibles, foco visible y objetivos táctiles mínimos de 44 px.

## Regla de visualización única

La confirmación se almacena en el servidor y se asocia al `Repartidor`, no al navegador ni al dispositivo.

El diseño utiliza un campo nullable de fecha y hora específico para esta actualización. La elegibilidad se calcula con dos condiciones:

1. el usuario ya existía antes o en el instante de lanzamiento definido por la aplicación;
2. el campo de confirmación permanece vacío.

Consecuencias:

- un repartidor ausente el día del despliegue lo verá en su siguiente inicio de sesión;
- cambiar de teléfono, borrar el almacenamiento o reinstalar la PWA no hace reaparecer el tutorial confirmado;
- un repartidor creado después del lanzamiento no recibe una actualización histórica;
- confirmar varias veces es idempotente.

El endpoint de perfil devuelve una bandera explícita para mostrar el tutorial. Un endpoint autenticado registra la confirmación únicamente para el repartidor de la sesión.

## Flujo de datos

1. La PWA autentica la sesión.
2. La PWA solicita el perfil operativo.
3. El backend resuelve el repartidor efectivo y devuelve `mostrar_tutorial_carga_sucursal`.
4. Si la bandera es verdadera, la PWA abre la hoja antes de mostrar el panel operativo.
5. Navegar, cerrar o elegir `Ahora no` no realiza ninguna escritura.
6. `Entendido, comenzar` envía una acción idempotente al backend.
7. El backend registra la fecha de confirmación y devuelve éxito.
8. La PWA cierra la hoja y continúa al panel.

Si la confirmación no puede enviarse por falta de conexión, la PWA no detiene el trabajo. Oculta el tutorial durante la sesión actual y coloca la confirmación en la cola offline existente. Hasta que se sincronice, el aviso podría aparecer en otro dispositivo.

## Relación con la liberación de ruta

En un tramo con dos sucursales, guardar solo una mantiene el bloqueo de salida. Cuando todas las líneas vigentes de ambas sucursales quedan cargadas:

- el checklist del tramo deja de bloquear la salida;
- se habilita `Salir a ruta`;
- el repartidor aún debe tener bitácora abierta en la misma unidad;
- la ruta debe estar planeada;
- el repartidor debe pulsar explícitamente `Salir a ruta`.

La transición resultante es:

`Todas las sucursales guardadas -> Tramo completo -> Salir a ruta habilitado -> Confirmación del repartidor -> EN_RUTA`

## Manejo de errores

- Un fallo al consultar el estado del tutorial no bloquea el inicio de sesión; se registra y la PWA continúa.
- Un fallo de confirmación mantiene la acción en cola para reintento.
- El botón final se bloquea mientras procesa para evitar doble envío.
- La respuesta repetida del endpoint conserva el mismo resultado exitoso.
- El tutorial no altera el contexto operativo, el checklist ni las cantidades capturadas.

## Pruebas

### Backend

- Repartidor elegible y sin confirmación recibe la bandera activa.
- Confirmación guarda fecha y desactiva la bandera.
- Confirmación repetida es idempotente.
- Otro usuario no puede confirmar por un repartidor distinto.
- Repartidor creado después del lanzamiento no recibe el tutorial.
- Usuario sin perfil de repartidor no recibe el tutorial.

### PWA

- Inicio de sesión elegible abre la hoja.
- `Ahora no`, Escape y cierre no confirman.
- `Entendido, comenzar` confirma y no reaparece en una sesión nueva.
- Atrás, Siguiente, gesto, puntos de avance y foco funcionan.
- La cola offline reintenta la confirmación.
- No existen errores de consola ni solicitudes fallidas inesperadas.

### Liberación de ruta

- Una sucursal pendiente mantiene `Salir a ruta` bloqueado.
- Todas las sucursales guardadas habilitan la acción.
- Guardar la última sucursal no cambia automáticamente a `EN_RUTA`.
- Sin bitácora o con unidad distinta la liberación continúa bloqueada.

### Visual y producción

- Verificación en 360, 390 y 430 px.
- Verificación con movimiento reducido y navegación por teclado.
- Incremento de versión del Service Worker en el mismo cambio.
- Prueba de despliegue con una cuenta controlada que no consuma la confirmación de usuarios reales.
- Validación final en la PWA real y revisión de consola, Network/XHR, caché y sesión.

## Mockups aprobados

Los mockups aislados del proceso de ideación muestran:

- los cinco pasos completos del tutorial;
- la hoja inferior con fondo desenfocado;
- la explicación separada de diferencias;
- el cierre de tramo con `Salir a ruta`.

Los mockups son referencia de comportamiento y jerarquía. La implementación debe reutilizar los tokens y componentes reales de la PWA en lugar de copiar estilos independientes.
