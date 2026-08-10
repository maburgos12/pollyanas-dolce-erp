# Captura operativa de bonos y horas extra para Rosa y Julissa

## Objetivo

Ampliar el perfil limitado `bonos_produccion_captura` para que Rosa Icela y
Julissa puedan registrar horas extra y la información diaria que alimenta los
bonos de Producción, sin convertirlas en administradoras de bonos ni permitirles
autorizar, configurar periodos o modificar importes de nómina.

## Alcance aprobado

La PWA de Bonos Producción mostrará cuatro apartados al perfil limitado:

- Permisos.
- Préstamos.
- Horas extras.
- Bonos, presentado como captura diaria.

Las operadoras podrán trabajar con ellas mismas y con personal activo cuyo
departamento actual u origen sea Producción y que tenga una jefatura activa con
usuario ERP. Carolina seguirá siendo la jefa/autorizadora según la relación
`Empleado.jefe_directo`.

Quedan fuera del alcance del perfil limitado:

- Configuración o creación de periodos.
- Resumen mensual y reportes de importes.
- Revisión administrativa por área.
- Ajuste positivo, ajuste negativo y bono extra.
- Autorización, rechazo, edición o eliminación de horas extra.
- Administración o autorización de permisos y préstamos.
- Cualquier módulo general del ERP fuera de la PWA acotada.

## Alternativas consideradas

### 1. Añadir el grupo general `PRODUCCION`

Se descarta porque reabriría las rutas administrativas que se retiraron de
Julissa y mezclaría captura con gestión y autorización.

### 2. Abrir los endpoints administrativos actuales al grupo limitado

Se descarta porque los serializadores actuales de bonos exponen importes y los
viewsets permiten operaciones más amplias. Condicionar cada acción dentro de esos
contratos aumentaría el riesgo de una omisión futura.

### 3. Contratos de captura separados y mínimos

Es la alternativa seleccionada. Mantiene intacta la administración existente y
añade endpoints específicos para lectura/captura operativa, con serializadores de
campos permitidos y pruebas negativas para las acciones prohibidas.

## Arquitectura

### Identidad y autorización

El acceso seguirá determinado por `is_bonos_produccion_capture_only(user)`, que
exige que el usuario pertenezca únicamente a `bonos_produccion_captura`, no sea
staff/superuser y no tenga accesos explícitos a módulos. La identidad laboral se
resolverá mediante `Empleado.usuario_erp`.

No se cambiarán grupos ni registros de empleados durante el despliegue de código:
Rosa y Julissa ya comparten el perfil limitado. El cambio de comportamiento será
consecuencia del contrato del grupo, no de permisos individuales difíciles de
auditar.

### Horas extras

`HorasExtraProduccionEquipoViewSet` aceptará al perfil limitado mediante
`CanAccessBonosProduccion`. Para una operadora:

- el catálogo se limitará a `empleados_operables_solicitudes_produccion()`;
- podrá crear una solicitud para sí misma o para cualquiera de esos empleados;
- el registro nacerá pendiente y su `jefe_directo` se derivará del empleado;
- se notificará a la jefatura con el flujo existente;
- `puede_gestionar`, `puede_editar`, `puede_eliminar` y `puede_autorizar` serán
  falsos;
- editar, eliminar, autorizar y rechazar devolverán 403;
- la respuesta operativa omitirá el monto calculado y datos de autorización que
  no necesita para capturar.

La lista podrá mostrar las horas del personal operable para evitar duplicados por
empleado y fecha, pero solo con fecha, horas, motivo, estado y folio.

### Captura diaria de bonos

Se crearán endpoints exclusivos del perfil limitado para:

1. listar las fichas mínimas de empleados que ya pertenecen al periodo mensual de
   bonos;
2. listar sus registros diarios;
3. crear o actualizar un día de captura.

Los campos aceptados serán únicamente:

- día;
- asistencia;
- uniforme;
- puntualidad;
- cumplimiento de producción;
- cantidad de pasteles embetunados cuando aplique;
- observación.

La escritura conservará `capturado_por` y llamará al servicio existente de
recalculo. El cliente no podrá enviar ni recibir `bono_extra`, ajustes, premio,
total a pagar, salario o cualquier otro importe. Tampoco podrá crear empleados de
periodo, borrar registros diarios ni operar periodos diferentes del mes/año
solicitado mediante identificadores fuera del queryset autorizado.

La administración actual seguirá usando sin cambios
`BonoProduccionViewSet`, `RegistroDiarioViewSet` y
`ConfigBonoPeriodoViewSet`.

### Interfaz PWA

Para `OPERADOR_SOLICITUDES`, la navegación quedará en:

1. Bonos.
2. Horas extras.
3. Permisos.
4. Préstamos.

El apartado Bonos reutilizará la interacción de captura diaria, pero sin tarjetas
de importes, configuración, resumen, ajustes ni revisión. El mes y año actuales
se mostrarán para dar contexto y no serán editables para este perfil; la carga
usará exclusivamente los endpoints operativos y el periodo correspondiente a la
fecha local del servidor.

Cada acción bloqueará solo el control presionado, impedirá doble envío y mostrará
el resultado mediante el estado accesible ya usado por la PWA. Los errores
conservarán el contexto para permitir reintento.

Como cambia una plantilla servida por Service Worker, se incrementará `CACHE_NAME`
en el mismo commit y se verificará el contenido servido después del despliegue.

## Reglas de datos y errores

- Un empleado fuera de Producción activa o sin jefatura ERP válida devolverá
  400/403 y no creará registros.
- Una hora extra duplicada para el mismo empleado y fecha seguirá devolviendo 400.
- Una ficha o registro diario fuera del periodo/catálogo autorizado devolverá
  404/403.
- Campos monetarios o no permitidos serán rechazados por el serializador acotado
  y nunca llegarán al modelo.
- No se modificarán migraciones, modelos, datos históricos ni valores de
  `bono_extra`, `ajuste_positivo` o `ajuste_negativo`.

## Pruebas y aceptación

Las pruebas automatizadas deberán demostrar, primero en rojo y luego en verde:

- Rosa/Julissa ven las cuatro pestañas y no ven pestañas administrativas.
- Ambas pueden crear horas extra propias y para personal válido de Producción.
- Las horas quedan pendientes y dirigidas a Carolina.
- Ninguna operadora puede editar, eliminar, autorizar o rechazar horas extra.
- Las respuestas de horas y bonos no exponen importes.
- Ambas pueden listar fichas del periodo y crear/actualizar captura diaria válida.
- La captura diaria recalcula mediante el servicio existente sin alterar los tres
  campos monetarios manuales.
- No pueden crear periodos, administrar bonos, usar los endpoints administrativos
  ni capturar para empleados fuera del alcance.
- Carolina conserva sus rutas y acciones administrativas actuales.
- Dirección General conserva sus rutas y acciones actuales.
- El Service Worker contiene una versión nueva de caché.

Antes del merge se ejecutarán `manage.py check`, `migrate --check`, pruebas
focalizadas de Bonos/RRHH y la regresión completa disponible en CI. En producción
se hará respaldo verificable, deploy con `scripts/deploy_web_safe.sh` y validación
autenticada de Rosa, Julissa, Carolina y Dirección sin crear solicitudes reales ni
alterar capturas reales.

## Despliegue y reversión

El cambio seguirá PR a `main`, deploy seguro y comprobación de la PWA/Service
Worker. La validación productiva de escritura se limitará a verificaciones
transaccionales con rollback o a pruebas de permisos sin persistencia.

Si falla la validación, la reversión será el redeploy del commit anterior. Al no
haber migraciones ni cambios masivos de datos, la reversión de código no requiere
transformaciones de base de datos.
