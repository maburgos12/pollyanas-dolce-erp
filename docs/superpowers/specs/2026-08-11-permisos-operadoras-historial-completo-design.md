# Historial de permisos para operadoras de Produccion

## Objetivo

Permitir que Rosa Icela y cualquier operadora con el rol limitado de captura consulte sus permisos personales y los que ella misma registro en tres alcances: el mes actual, el mes anterior y todo su historial. La consulta no debe ampliar facultades de autorizacion, edicion, eliminacion ni rechazo.

## Experiencia aprobada

La tarjeta `Permisos registrados` mostrara, solo para operadoras, un control segmentado con estas opciones:

- `Este mes`, seleccionada al abrir la pantalla.
- `Mes anterior`, incluyendo correctamente el cambio de enero al diciembre del anio previo.
- `Todo el historial`, ordenado de la fecha mas reciente a la mas antigua.

El titulo de la tarjeta indicara el alcance activo: mes y anio concretos o `Todo el historial`. La fecha completa de cada permiso seguira visible en su tarjeta.

Si la operadora registra un permiso nuevo mientras consulta otro alcance, la pantalla regresara a `Este mes` y recargara ese periodo para que el registro recien creado sea visible de inmediato.

La vista administrativa de Carolina y los demas responsables conservara el selector y comportamiento actuales; los nuevos controles no apareceran fuera del perfil limitado de captura.

## Arquitectura y flujo de datos

No se agregaran modelos, migraciones ni nuevos endpoints. El endpoint existente `GET /api/bonos-produccion/permisos/` ya admite el contrato necesario:

- `mes` y `anio` actuales para `Este mes`.
- `mes` y `anio` calculados para `Mes anterior`.
- Sin `mes` ni `anio` para `Todo el historial`.

En los tres casos el servidor mantendra como autoridad el filtro de minimo privilegio: union de permisos capturados por el usuario autenticado y permisos personales ligados a su empleado. La interfaz no intentara filtrar permisos ajenos ni dependera de datos descargados de otra operadora.

El componente `PermisosTab` mantendra un estado local de alcance. La carga aceptara un alcance explicito para evitar condiciones de carrera cuando una solicitud nueva cambie la seleccion a `Este mes` y recargue la lista.

## Estados y errores

- Durante cada cambio de alcance se conservara el indicador `Cargando permisos...`.
- Si la consulta falla, se mantendra el toast global existente y la operadora podra reintentar seleccionando nuevamente el alcance.
- Un historial vacio mostrara el mensaje ya acotado a permisos personales o capturados.
- Ningun cambio de alcance enviara solicitudes POST ni modificara permisos.

## Seguridad y compatibilidad

- Las operadoras continuaran sin acciones `Autorizar`, `Rechazar`, `Editar` o `Eliminar`.
- Carolina conservara la autorizacion de permisos y la primera autorizacion de prestamos; Direccion General conservara el cierre de prestamos.
- El cambio visible incluira un incremento de `CACHE_NAME` en el service worker de Bonos Produccion.
- No se cambiaran estados, folios, capturistas, relaciones laborales, bonos ni datos de nomina.

## Validacion

1. Prueba automatizada de consulta sin mes/anio que incluya registros de varios meses del mismo capturista y excluya los de otra operadora.
2. Pruebas de interfaz para las tres opciones, el alcance predeterminado, el cruce enero-diciembre y el titulo activo.
3. Prueba de que las acciones administrativas siguen ausentes para la operadora.
4. Verificacion de la nueva version de caché PWA.
5. Suite de regresion de `bonos_produccion`, `bonos_ventas` y `rrhh`, ademas de `check` y `migrate --check`.
6. Navegador local con registros del mes actual, anterior y otra operadora.
7. Tras PR, CI y respaldo integro, validacion productiva de solo lectura con sesion efimera de Rosa; no se crearan solicitudes reales.

## Fuera de alcance

- Busqueda por texto, filtros por empleado o estado y paginacion avanzada.
- Cambios en las pantallas administrativas de permisos.
- Cambios en la cadena de autorizacion o en el modelo de datos.
