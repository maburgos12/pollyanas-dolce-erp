# App Operativa: fallas y mermas de insumos

Fecha: 2026-07-22  
Estado: diseño aprobado por Mauricio  
Alcance: especificación funcional y técnica; no autoriza todavía cambios de código, datos ni producción.

## 1. Objetivo

Complementar App Operativa con dos capacidades para las sucursales:

1. Reportar fallas de equipos registrados o de las instalaciones de su tienda. Los reportes llegan directamente a Mantenimiento, que es responsable de asignarlos, darles seguimiento y cerrarlos.
2. Registrar mermas de los insumos que la sucursal realmente recibe. El jefe inmediato definido en RRHH revisa la merma y, si la aprueba, el ERP genera una orden segura para que el agente ajuste la existencia en Point.

El diseño preserva las fuentes de verdad: Point es dueño de insumos, unidades, recepciones y existencias; RRHH es dueño de la relación de jefe inmediato; Activos es dueño de los equipos; Mantenimiento es dueño del seguimiento de fallas; el ERP conserva solicitud, autorización, ejecución y auditoría.

## 2. Alcance

### Incluido

- Nuevas entradas móviles dentro de App Operativa.
- Fallas de equipos pertenecientes a la sucursal activa.
- Fallas de instalaciones sin crear activos ficticios.
- Merma de insumos elegibles según recepciones, movimientos y existencia de Point.
- Aprobación por el jefe inmediato vigente en RRHH.
- Ejecución automática protegida en Point mediante el agente.
- Notificaciones, historial, trazabilidad e informes de conciliación.
- Despliegue gradual con simulación y piloto inicial en Payán.

### Fuera de alcance

- Sustituir la merma actual de producto terminado enviada a CEDIS.
- Crear un catálogo alterno de insumos o unidades.
- Permitir que sucursales vean todo el catálogo del almacén.
- Aprobar automáticamente cuando no exista jefe inmediato.
- Ajustes parciales, inventario negativo o cambios de unidad.
- Integración con WhatsApp en la primera versión.
- Rediseñar el flujo interno completo de Mantenimiento.

## 3. Arquitectura recomendada

### 3.1 Fallas

Se reutiliza el dominio existente `fallas.ReporteFalla` y su integración con Mantenimiento. App Operativa ofrece dos tipos explícitos de objetivo:

- **Equipo registrado:** requiere un `activos.Activo` activo cuya sucursal coincida con la sucursal operativa de la sesión.
- **Instalación:** no requiere activo; exige área física y categoría de falla.

Ambos crean un reporte en la bandeja actual de Mantenimiento. No pasan por el jefe inmediato. Mantenimiento asume revisión, asignación, seguimiento, comunicación, costos, proveedor, resolución y cierre.

### 3.2 Merma de insumos

Se crea un agregado independiente para merma de insumos. No se amplía `mermas.MermaRegistro`, porque ese modelo representa producto terminado, traslado por repartidor y recepción física en CEDIS.

El nuevo agregado se conecta con:

- Point, para identidad del insumo, unidad, recepción, movimiento y existencia.
- RRHH, para resolver al jefe inmediato.
- Orquestación, para generar y vigilar la orden del agente.
- App Operativa, para captura, aclaración y consulta de estado.

La ejecución en Point se representa mediante una orden separada uno a uno con la merma aprobada. La orden contiene un payload inmutable y una clave única de idempotencia.

## 4. Experiencia en App Operativa

### 4.1 Inicio

La pantalla principal conserva acciones separadas:

- `Reportar una falla`
- `Merma de producto` — flujo actual hacia CEDIS
- `Merma de insumo` — flujo nuevo

La sucursal proviene de la sesión y nunca se elige libremente por una usuaria de tienda.

### 4.2 Reportar una falla

El primer control pregunta `¿Qué falló?` y ofrece:

- `Equipo`: selector limitado a los activos registrados en la sucursal.
- `Instalación`: selector de área física y categoría.

Campos comunes:

- Categoría.
- Prioridad.
- Título y descripción.
- Foto opcional.
- Justificación obligatoria cuando no se adjunte foto.

La acción principal dice `Enviar a Mantenimiento`. Al enviarse, el reporte aparece inmediatamente en la bandeja de Mantenimiento.

### 4.3 Registrar merma de insumo

Campos:

- Insumo elegible recibido por la sucursal.
- Existencia de Point mostrada como referencia de solo lectura.
- Cantidad decimal en la unidad oficial de Point.
- Equivalencia visual opcional, sin cambiar la unidad enviada.
- Motivo.
- Comentario.
- Foto opcional.
- Justificación obligatoria cuando no se adjunte foto.

La acción principal dice `Enviar a mi jefe`.

### 4.4 Comportamiento de acciones

Todas las acciones nuevas usan el contrato compartido `data-async-action`:

- Solo se bloquea el botón presionado.
- El botón muestra `Enviando…` o `Procesando…`.
- Se previene el doble envío.
- El resultado se comunica mediante toast global accesible.
- La pantalla conserva posición y contexto.
- Ante error se conservan los campos y se permite reintentar.
- El fallback POST tradicional regresa a un identificador estable.

Las pantallas incluyen `Mis fallas` y `Mis mermas` para consultar el estado vigente y el historial visible para la sucursal.

## 5. Elegibilidad de insumos

El selector se construye desde datos de Point; no replica el catálogo general del almacén.

Un insumo es elegible cuando:

- existe evidencia de que la sucursal lo recibió; y
- Point informa existencia positiva; o
- han transcurrido siete días o menos desde su última recepción o movimiento después de quedar en cero; o
- existe una excepción administrativa vigente y auditada.

Cuando desaparece del selector, su historial no se elimina. Las excepciones deben registrar motivo, creador, vigencia y revocación.

La identidad se basa en el código estable de Point. El nombre y la unidad se guardan además como fotografía histórica del momento de la captura.

## 6. Flujo de autorización

### 6.1 Resolución del jefe

Al enviar la merma, el ERP resuelve al jefe inmediato vigente definido en RRHH y guarda esa relación en el expediente.

Si falta el jefe, está inactivo o la relación es inconsistente:

- no se aprueba automáticamente;
- no se crea una orden para el agente;
- la solicitud pasa a `Sin responsable asignado`;
- Administración o Dirección puede asignar al responsable correcto;
- la reasignación queda auditada.

El aprobador debe tener autoridad sobre la persona reportante y alcance sobre la sucursal. Ser jefe en otra área no concede acceso.

### 6.2 Decisiones permitidas

El jefe inmediato puede:

- **Aprobar:** acepta exactamente la cantidad reportada.
- **Aprobar una cantidad menor:** exige justificación y conserva ambas cantidades.
- **Solicitar aclaración:** devuelve la solicitud sin perder datos ni evidencia.
- **Rechazar:** exige motivo y no genera orden.

El jefe nunca puede aumentar la cantidad. La sucursal debe corregirla y reenviarla; cualquier cambio de insumo, sucursal o cantidad requiere una nueva aprobación.

## 7. Estados

Flujo principal:

`BORRADOR → ENVIADA → APROBADA → EJECUTANDO → APLICADA`

Derivaciones de revisión:

- `SIN_RESPONSABLE`
- `EN_ACLARACION`
- `RECHAZADA`
- `REQUIERE_REVISION`
- `INTERVENCION_TECNICA`

`REQUIERE_REVISION` cubre existencia insuficiente, unidad incompatible, identidad dudosa o cambio material en Point. `INTERVENCION_TECNICA` cubre fallos persistentes del portal, sesión o automatización después de agotar reintentos seguros.

Cada transición crea un evento inmutable con actor, fecha, estado anterior, estado nuevo, motivo y cambios relevantes.

## 8. Datos y contratos

### 8.1 Expediente de merma

Debe conservar como mínimo:

- sucursal y usuario reportante;
- código estable, nombre y unidad de Point;
- cantidad reportada y cantidad aprobada;
- motivo, comentario, evidencia y justificación sin foto;
- jefe inmediato resuelto y eventuales reasignaciones;
- estado y fechas de transición;
- referencia a la orden de ajuste, cuando exista.

Las cantidades usan `Decimal` con precisión compatible con Point. No se convierten mediante valores de punto flotante.

### 8.2 Orden de ajuste

Debe conservar:

- merma aprobada de origen;
- payload inmutable: sucursal Point, código de insumo, unidad y cantidad negativa aprobada;
- clave única de idempotencia;
- estado de ejecución, número de intentos y último error;
- existencia observada antes y después;
- folio o referencia de Point;
- fecha de aplicación y evidencia técnica.

Una merma solo puede tener una orden activa. Una orden aplicada no puede volver a ejecutarse.

## 9. Ejecución segura del agente

Antes de modificar Point, el agente debe comprobar:

1. La solicitud continúa aprobada y su contenido coincide con el payload firmado o protegido de la orden.
2. La orden no está aplicada y no existe ya en Point un movimiento con su referencia idempotente.
3. La sucursal y el código identifican exactamente al insumo aprobado.
4. La unidad coincide con la unidad oficial.
5. La existencia actual es igual o mayor que la cantidad aprobada.

Si la existencia es menor, el agente no ajusta parcialmente ni permite existencia negativa. La orden pasa a `REQUIERE_REVISION` y el jefe puede devolverla para corrección y nueva aprobación.

Ante error técnico, el agente conserva la misma orden y clave, consulta primero si el movimiento ya fue aplicado y reintenta un número limitado de veces con espera progresiva. Si no logra confirmar un resultado seguro, pasa a `INTERVENCION_TECNICA`.

El ERP solo muestra `APLICADA` cuando Point devuelve una confirmación verificable y se ha persistido la existencia posterior o evidencia equivalente.

## 10. Notificaciones

Primera versión:

- Aviso dentro de App Operativa y ERP.
- Correo al jefe inmediato al recibir una merma.
- Aviso a la sucursal cuando se solicita aclaración, se aprueba o se rechaza.
- Aviso a Mantenimiento al recibir una falla nueva, con énfasis adicional para prioridad crítica.
- Aviso técnico cuando una orden requiera intervención.

No se incluye WhatsApp en esta etapa.

## 11. Seguridad y permisos

- La sucursal operativa se deriva del alcance de la sesión.
- Ninguna sucursal puede consultar equipos, insumos, fallas o mermas de otra.
- El servidor vuelve a validar pertenencia de activo e insumo aunque la interfaz ya los filtre.
- Solo el jefe inmediato resuelto o un rol explícito de excepción puede decidir la merma.
- Los roles de excepción no pueden ocultar ni reescribir el historial.
- Evidencias y respuestas no exponen rutas internas, credenciales ni datos del agente.
- Toda operación sensible registra usuario, IP/contexto disponible y marca temporal.

## 12. Manejo de errores

- Error de validación: conservar captura y señalar el campo exacto.
- RRHH sin jefe: `SIN_RESPONSABLE`, sin orden ni aprobación automática.
- Insumo dejó de ser elegible antes de enviar: bloquear envío y refrescar información de Point.
- Existencia insuficiente al ejecutar: `REQUIERE_REVISION`, sin movimiento parcial.
- Unidad o identidad inconsistente: `REQUIERE_REVISION`, sin inferencias por nombre.
- Timeout o sesión Point expirada: reintento seguro después de consultar si ya existe el movimiento.
- Resultado ambiguo: no declarar éxito; escalar a intervención técnica.
- Notificación fallida: no revertir la transición de negocio; registrar y reintentar el aviso.

## 13. Validación

### 13.1 Pruebas automáticas

- Aislamiento estricto por sucursal.
- Equipos limitados a activos registrados de la sucursal.
- Instalaciones válidas sin activo ficticio.
- Foto opcional con justificación obligatoria cuando falta.
- Elegibilidad desde recepciones, existencia, tolerancia de siete días y excepciones.
- Unidad oficial inmutable y cantidades decimales.
- Resolución del jefe desde RRHH y bandeja sin responsable.
- Acciones permitidas; prohibición de aumentar cantidad.
- Nueva aprobación después de cambios materiales.
- Transiciones válidas y eventos inmutables.
- Una sola orden por merma y aplicación idempotente.
- Rechazo de ajustes parciales o negativos.
- Reintentos que consultan primero el resultado remoto.
- Notificaciones y escalamiento técnico.
- Compatibilidad con el flujo actual de mermas de producto y con Mantenimiento.

### 13.2 Validación visible

- Navegador móvil real en App Operativa.
- Consola sin errores y solicitudes XHR/Fetch correctas.
- Botones, doble envío, toasts, foco, conservación de posición y reintento.
- Sesión y alcance real de sucursal.
- Service Worker actualizado mediante bump de `CACHE_NAME` cuando cambien templates o estáticos.
- Bandeja real de Mantenimiento para ambos tipos de falla.
- Correspondencia entre orden ERP y movimiento Point.

## 14. Liberación gradual

### Etapa 1: captura y base

Habilitar Fallas y Merma de insumos en App Operativa. Validar sucursal, equipos, insumos elegibles, unidad, evidencia y jefe inmediato.

### Etapa 2: agente en simulación

Las aprobaciones son reales, pero el agente solo consulta Point y registra el ajuste que habría realizado. No modifica existencia.

### Etapa 3: piloto en Payán

Habilitar ajustes reales solo para Payán mediante configuración explícita. Conciliar diariamente:

- total aprobado;
- total aplicado;
- órdenes pendientes;
- diferencias entre ERP y Point.

### Etapa 4: expansión

Incorporar sucursales gradualmente después de comprobar que no existen duplicados, unidades incorrectas, movimientos ambiguos ni existencias negativas.

Cada etapa exige PR revisado, despliegue mediante `scripts/deploy_web_safe.sh` y validación visible en producción. No se activa la etapa siguiente únicamente por haber pasado pruebas locales.

## 15. Criterios de aceptación

- Una sucursal reporta una falla sobre un equipo propio o una instalación y Mantenimiento la recibe directamente.
- Una sucursal solo puede seleccionar insumos que Point confirma que recibió y que cumplen la política de visibilidad.
- El jefe inmediato definido en RRHH es el único aprobador ordinario.
- Una aprobación crea una orden inmutable e idempotente para el agente.
- Point nunca recibe un ajuste parcial, duplicado, con unidad incompatible o que produzca existencia negativa.
- El ERP conserva cantidades reportada y aprobada, todas las decisiones y la evidencia de Point.
- Los errores técnicos se reintentan con seguridad y los resultados ambiguos nunca se presentan como éxito.
- El flujo actual de merma de producto terminado y el seguimiento actual de Mantenimiento continúan funcionando.

