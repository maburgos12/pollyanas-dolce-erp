# Visitas a sucursal: plan mensual y ejecución flexible con evidencia GPS

Fecha: 2026-07-31
Estado: diseño aprobado por Mauricio
Alcance: especificación funcional y técnica; no autoriza todavía cambios de código, datos ni producción.

## 1. Objetivo

Convertir el cronograma de visitas a sucursal en un plan mensual de compromisos, sin obligar al auditor a ejecutar cada visita exactamente en la fecha planeada.

El sistema debe conservar por separado:

- la fecha programada, como compromiso del plan;
- la fecha y hora reales, como evidencia de ejecución;
- la sucursal seleccionada;
- el auditor que realmente ejecutó;
- la ubicación GPS y su precisión;
- la distancia calculada por el servidor contra la geocerca de la sucursal.

Una visita programada puede cumplirse antes, el mismo día o después. La diferencia de fechas nunca debe bloquear la captura. Si el auditor visita una sucursal sin vincular la ejecución con una programación pendiente, la auditoría se registra como extraordinaria y exige una causa.

## 2. Principios del contrato

1. El cronograma representa el plan; no es una cita rígida.
2. Ejecutar una auditoría nunca mueve ni reemplaza su `fecha_programada`.
3. La ejecución registra `fecha_real` y `realizada_en` con fecha y hora del servidor.
4. Una programación solo puede cumplirse una vez.
5. El auditor elige explícitamente la sucursal y, después, la programación pendiente que está cumpliendo.
6. Si no corresponde a una programación, puede iniciar una auditoría extraordinaria.
7. Las extraordinarias son actividad válida y no requieren autorización previa.
8. Una extraordinaria no consume una programación ni aumenta artificialmente el cumplimiento del plan.
9. La diferencia entre fecha planeada y real se informa; no se considera por sí misma un error.
10. El cierre exige GPS válido y comprobación contra la geocerca de la sucursal seleccionada.

## 3. Alcance

### Incluido

- Planificación mensual mediante el cronograma existente.
- Selección de sucursal y programación pendiente desde la app del auditor.
- Ejecución anticipada, puntual o atrasada.
- Auditorías extraordinarias por seguimiento, queja u otro motivo.
- Evidencia GPS obligatoria con precisión máxima de 100 metros.
- Validación de geocerca calculada en el servidor.
- Representación separada de fecha programada y fecha real.
- Indicadores de cumplimiento, puntualidad y actividad extraordinaria.
- Protección contra doble envío y concurrencia entre auditores.
- Conservación de la captura cuando la ubicación no pueda validarse.
- Validación de permisos tanto en navegación como en acceso directo.

### Fuera de alcance

- Crear una fuente de sucursales o geocercas distinta de `core.Sucursal` y `logistica.PuntoLogistico`.
- Rastrear continuamente al auditor antes o después de la visita.
- Autorizar extraordinarias mediante un flujo previo de aprobación.
- Garantizar criptográficamente que un dispositivo no fue manipulado para falsificar GPS.
- Eliminar o reinterpretar registros históricos existentes.
- Cambiar permisos de empleados de sucursal o revivir el flujo eliminado de bitácoras de sucursal.
- Rediseñar otros módulos de App Operativa.

## 4. Modelo operativo

### 4.1 Plan mensual

El plan mensual es el conjunto de `VisitaSucursal` no extraordinarias cuya `fecha_programada` pertenece al mes seleccionado. No se crea un modelo paralelo de “Plan mensual”. El cronograma permite agregar, consultar, reprogramar o cancelar compromisos pendientes y deriva sus totales de esas visitas.

Una reprogramación administrativa explícita puede seguir existiendo únicamente mientras la visita esté pendiente. Debe registrar fecha anterior, fecha nueva, actor y motivo. La ejecución nunca debe invocar esa reprogramación automáticamente.

### 4.2 Cumplimiento flexible

Después de seleccionar una sucursal, la app muestra sus programaciones pendientes, agrupadas y ordenadas así:

1. vencidas, de la más antigua a la más reciente;
2. programadas para hoy;
3. futuras, de la más próxima a la más lejana.

El auditor selecciona explícitamente cuál compromiso cumplirá. Puede elegir una programación vencida o futura; no existe una ventana temporal que bloquee la acción.

Al cerrar:

- `fecha_programada` permanece sin cambios;
- `fecha_real` recibe la fecha local del servidor;
- `realizada_en` recibe la marca temporal completa;
- `realizada_por` recibe el usuario autenticado;
- el estado cambia a `REALIZADA`;
- la programación deja de aparecer entre las pendientes.

El campo `auditor` continúa siendo una asignación planeada opcional. No restringe la ejecución a esa persona. El actor real y auditable siempre es `realizada_por`.

### 4.3 Auditoría extraordinaria

La app ofrece `Auditoría extraordinaria` como alternativa explícita a las programaciones pendientes. También está disponible cuando no existe ninguna programación para la sucursal.

Causas:

- `SEGUIMIENTO_PENDIENTES` — seguimiento a hallazgos o compromisos anteriores;
- `QUEJA` — revisión originada por una queja;
- `OTRO` — cualquier otra causa operativa.

Toda extraordinaria exige una explicación. Puede haber varias extraordinarias para la misma sucursal y fecha; cada una conserva checklist, evidencia, actor, hora y GPS propios.

## 5. Diseño de datos

Se reutiliza `visitas_sucursal.VisitaSucursal` como expediente único de planificación y ejecución. No se crea una tabla separada de ejecuciones porque una programación solo admite un cumplimiento.

### 5.1 Cambios propuestos

- Hacer `fecha_programada` opcional únicamente para auditorías `EXTRAORDINARIA`.
- Agregar estado `BORRADOR` para una extraordinaria iniciada pero todavía no cerrada.
- Agregar `motivo_extraordinaria` con las causas definidas.
- Agregar `detalle_extraordinaria` para la explicación obligatoria.
- Agregar `gps_radio_geocerca_m` para conservar el radio aplicado al momento del cierre.
- Conservar los campos existentes `fecha_real`, `realizada_en`, `realizada_por`, `gps_latitud`, `gps_longitud`, `gps_precision_m`, `gps_distancia_sucursal_m` y `gps_dentro_geocerca`.
- Exponer una propiedad o función de dominio `desviacion_dias = fecha_real - fecha_programada` solo cuando existan ambas fechas.

### 5.2 Invariantes

- Una visita no extraordinaria requiere `fecha_programada`.
- Una extraordinaria no finge una fecha programada.
- Una extraordinaria en borrador requiere sucursal, causa, detalle y creador.
- Una visita `REALIZADA` requiere `fecha_real`, `realizada_en`, `realizada_por`, coordenadas, precisión, distancia y resultado positivo de geocerca.
- `gps_precision_m` debe ser mayor que cero y menor o igual a 100.
- Una visita `CANCELADA` no puede ejecutarse.
- Una visita `REALIZADA` no puede ejecutarse nuevamente.
- Solo visitas no extraordinarias forman parte del denominador del plan mensual.

Las reglas se validan en el servicio de dominio para todo cierre nuevo y, cuando sean representables sin impedir una migración segura, mediante restricciones de base de datos. No se inventan coordenadas ni geocercas para registros históricos que carezcan de ellas.

### 5.3 Compatibilidad histórica

La migración vuelve nullable `fecha_programada`, pero no borra ni cambia fechas existentes. Antes de agregar restricciones condicionales se auditan los valores históricos. El registro histórico de CEDIS actualmente excluido del cronograma no se elimina ni se reutiliza; permanece fuera de las sucursales visitables.

El método `__str__`, ordenamientos, filtros, plantillas y exportaciones deben tolerar una extraordinaria sin `fecha_programada`.

## 6. Servicio de ejecución

La lógica de cierre debe concentrarse en un único servicio transaccional compartido por la respuesta HTML y JSON. Las vistas no duplican reglas de negocio.

Entrada mínima:

- usuario autenticado;
- visita programada o borrador extraordinario;
- sucursal seleccionada;
- respuestas del checklist;
- personal presente, observaciones y fotografías;
- latitud, longitud y precisión reportadas por el dispositivo.

Dentro de `transaction.atomic()` el servicio:

1. bloquea la visita con `select_for_update()`;
2. vuelve a validar permiso `MANAGE`;
3. confirma que la sucursal continúa siendo visitable;
4. confirma que el estado permite cerrar;
5. valida el tipo y, para extraordinarias, causa y explicación;
6. obtiene la geocerca canónica desde `PuntoLogistico`;
7. valida precisión GPS de hasta 100 metros;
8. calcula en el servidor la distancia entre coordenadas y geocerca;
9. exige que la ubicación esté dentro del radio configurado;
10. persiste checklist y evidencia validados;
11. registra fecha, hora, actor y datos GPS;
12. cambia el estado a `REALIZADA`;
13. registra el evento en la auditoría del ERP.

El cliente nunca decide si está dentro de la geocerca ni envía una distancia confiable. Solo envía la medición del dispositivo; el servidor calcula y decide.

## 7. GPS y evidencia de presencia

### 7.1 Requisitos de cierre

La auditoría no puede cerrarse cuando:

- el navegador no entrega coordenadas;
- la precisión falta, es cero o supera 100 metros;
- la sucursal no tiene geocerca configurada;
- la geocerca está incompleta;
- la coordenada queda fuera del radio configurado.

El error debe indicar cuál condición falló y permitir reintentar la medición sin perder el checklist. Mientras no exista un cierre válido, los valores permanecen en el formulario o borrador y no se presentan como una auditoría realizada.

### 7.2 Evidencia persistida

Se conserva:

- latitud y longitud;
- precisión reportada;
- distancia calculada por el servidor;
- radio de geocerca aplicado o una referencia estable al punto usado;
- fecha y hora del servidor;
- usuario ejecutor;
- sucursal y visita;
- fotografías y observaciones.

Guardar el radio o una fotografía histórica de la geocerca aplicada evita que un cambio posterior en `PuntoLogistico` vuelva ambiguo el resultado original.

### 7.3 Límite de la garantía

El GPS del navegador es evidencia operativa y permite comprobar coherencia con la sucursal, pero no demuestra por sí solo que el dispositivo no fue manipulado. La combinación de coordenadas, precisión, distancia calculada, fotografías, usuario y hora aumenta la trazabilidad sin afirmar una garantía técnica inexistente.

## 8. Experiencia del auditor

### 8.1 Entrada

La tarjeta `Auditorías` continúa apareciendo por permiso `MANAGE`, aunque todavía no existan programaciones. La app no debe confundir permiso con disponibilidad de visitas.

### 8.2 Flujo

1. Elegir sucursal.
2. Elegir una programación pendiente o `Auditoría extraordinaria`.
3. Si es extraordinaria, elegir causa y escribir explicación.
4. Capturar checklist, personal presente, observaciones y fotografías.
5. Activar ubicación.
6. Ver estado de precisión y validación de geocerca.
7. Presionar `Ejecutar auditoría`.
8. Recibir confirmación con sucursal, fecha real y referencia de la auditoría.

La fecha real, hora y usuario no son editables. La interfaz muestra la fecha programada como contexto, no como fecha obligatoria.

### 8.3 Acciones y errores

El cierre usa el contrato compartido `data-async-action`:

- solo bloquea el botón presionado;
- muestra `Validando ubicación…` y después `Guardando…`;
- previene doble envío;
- responde mediante el toast global accesible;
- conserva posición, sucursal, programación y datos capturados en el formulario o borrador;
- permite reintentar GPS o envío;
- ofrece el mismo resultado mediante fallback POST tradicional;
- vuelve a un identificador estable de la visita.

La creación de un borrador extraordinario usa una clave idempotente generada por el servidor. Repetir la solicitud no crea dos borradores.

## 9. Cronograma mensual

### 9.1 Representación

El cronograma no duplica registros. Proyecta la misma visita en la fecha planeada y, cuando difiere, en la fecha real.

Marcadores con texto accesible:

- `P` — programada y pendiente;
- `✓` — realizada en la fecha programada;
- `✓ +N` — realizada N días después;
- `✓ −N` — realizada N días antes;
- `E` — extraordinaria.

No se depende solo del color. Cada marcador tiene etiqueta, estado y texto para lector de pantalla.

### 9.2 Dos fechas, un expediente

Si una visita programada para el 10 se ejecuta el 12:

- el día 10 muestra `Cumplida el 12 · +2 días`;
- el día 12 muestra `Realizada el 12 · programada para el 10`;
- ambos enlaces abren el mismo `VisitaSucursal`;
- los totales cuentan una sola auditoría.

Si ambas fechas coinciden, se muestra un solo marcador combinado.

### 9.3 Cruce entre meses

Una visita programada el 31 de julio y ejecutada el 1 de agosto:

- pertenece al plan de julio y a su indicador de cumplimiento;
- aparece como ejecución real en la actividad de agosto;
- no se cuenta como programación de agosto.

Las consultas deben unir las proyecciones por fecha programada y fecha real, deduplicando por id.

## 10. Indicadores

Para el mes del plan:

- total programado;
- cumplidas;
- pendientes;
- vencidas;
- cumplimiento `cumplidas / programadas`;
- realizadas en fecha;
- anticipadas y promedio de anticipación;
- atrasadas y promedio de retraso.

Para la actividad real del mes:

- auditorías realizadas;
- programadas ejecutadas;
- extraordinarias;
- extraordinarias por causa y sucursal.

Las extraordinarias nunca forman parte del denominador del plan. Una visita ejecutada fuera del mes planeado sí cumple el plan del mes al que pertenece su `fecha_programada`.

## 11. Permisos y alcance

- Programar y ejecutar requiere `MANAGE` en `ventas.visitas_sucursal`.
- La tarjeta, los controles y las rutas directas aplican el mismo permiso.
- Un auditor autorizado puede ejecutar cualquier programación visitable, aunque el campo `auditor` señale a otra persona.
- La asignación planeada es informativa; `realizada_por` es la identidad efectiva.
- El servidor valida que la visita elegida corresponda a la sucursal seleccionada.
- CEDIS y sucursales no operativas continúan excluidas.
- Empleados de sucursal no reciben capacidad de captura por este cambio.

## 12. Concurrencia e idempotencia

- El cierre bloquea la fila de visita dentro de una transacción.
- Si ya está `REALIZADA` o `CANCELADA`, rechaza la segunda operación sin modificar evidencia.
- Si dos auditores abren la misma programación, el primero que cierre válidamente gana; el segundo recibe un aviso y la versión actualizada.
- El botón evita doble toque, pero la garantía principal vive en el servidor.
- El borrador extraordinario se identifica con una clave idempotente única.
- Fotografías y relaciones de personal solo se consolidan una vez después de validar el cierre.

## 13. Manejo de errores

- Permiso insuficiente: `403`, sin revelar datos de otras visitas.
- Programación inexistente o de otra sucursal: error específico, sin inferir una alternativa.
- Visita ya cumplida: informar quién y cuándo la realizó; no sobrescribir.
- GPS denegado o ausente: conservar captura y ofrecer reintento.
- Precisión mayor a 100 metros: pedir esperar y volver a medir.
- Geocerca inexistente: bloquear cierre e indicar que debe configurarse la sucursal.
- Fuera de geocerca: mostrar distancia calculada y sucursal esperada.
- Error al guardar: revertir el cierre transaccional, conservar el formulario o borrador y no mostrar la auditoría como realizada.
- Fotografía fallida: no declarar cierre completo si la evidencia requerida no quedó persistida.

Los errores inmediatos usan toast accesible y mensaje junto al control relacionado. El banner superior no es la única respuesta.

## 14. Arquitectura de implementación propuesta

Responsabilidades:

- `models.py`: campos, estados, invariantes simples y propiedades derivadas.
- servicio de dominio de visitas: selección de pendientes, creación idempotente de extraordinarias y cierre transaccional.
- `views.py`: autorización, parseo de entrada y adaptación HTML/JSON.
- cronograma: proyecciones de planificación y ejecución sin doble conteo.
- plantillas: selección progresiva, captura, estados GPS y representación accesible.
- pruebas: contrato de negocio, autorización, geocerca, concurrencia y presentación.

Consumidores que deben revisarse por el cambio nullable de `fecha_programada`:

- `VisitaSucursal.__str__` y ordenamiento;
- lista, detalle y app;
- cronograma móvil y escritorio;
- filtros, métricas y pruebas existentes;
- administración Django y cualquier exportación;
- creación de hallazgos y conversión a fallas.

La implementación probable toca:

- `visitas_sucursal/models.py`;
- una migración nueva, sin modificar migraciones aplicadas;
- `visitas_sucursal/views.py` y un servicio de dominio acotado;
- `visitas_sucursal/templates/visitas_sucursal/app.html`;
- `visitas_sucursal/templates/visitas_sucursal/lista.html`;
- estilos o JavaScript propios del módulo;
- `visitas_sucursal/tests.py` y pruebas específicas nuevas;
- `docs/ux/action-context-coverage.md`.

Si una PWA o Service Worker consume estas plantillas o estáticos, el mismo cambio debe incluir el bump coordinado de `CACHE_NAME`, pruebas y validación de actualización instalada.

## 15. Pruebas automáticas

### 15.1 Plan y fechas

- Ejecutar el mismo día conserva fecha programada y produce desviación cero.
- Ejecutar antes produce desviación negativa sin bloqueo.
- Ejecutar después produce desviación positiva sin bloqueo.
- Una visita realizada deja de aparecer entre pendientes.
- Dos programaciones de la misma sucursal se seleccionan explícitamente.
- Ejecutar una no consume la otra.
- Cruce de mes atribuye plan y actividad a sus meses correctos.

### 15.2 Extraordinarias

- Crear por seguimiento de pendientes.
- Crear por queja.
- Crear con otro motivo y explicación.
- Rechazar causa o explicación faltante.
- Permitir varias extraordinarias de una sucursal el mismo día.
- Excluir extraordinarias del denominador del plan.
- Reintentar creación con la misma clave sin duplicar.

### 15.3 GPS

- Aceptar precisión positiva de hasta 100 metros dentro de geocerca.
- Rechazar precisión ausente, cero o mayor a 100 metros.
- Rechazar coordenadas ausentes.
- Rechazar sucursal sin geocerca completa.
- Rechazar ubicación fuera de radio.
- Calcular distancia en servidor e ignorar cualquier distancia enviada por cliente.
- Conservar checklist ante fallo de GPS.
- Persistir fotografía histórica de la geocerca aplicada.

### 15.4 Seguridad e integridad

- Mostrar tarjeta solo con `MANAGE`.
- Rechazar acceso directo sin `MANAGE`.
- Rechazar visita de sucursal distinta a la seleccionada.
- Excluir CEDIS y sucursales no operativas.
- Registrar al ejecutor real aunque exista auditor planeado distinto.
- Impedir reejecución de visitas realizadas o canceladas.
- Resolver doble envío y concurrencia sin sobrescritura.

### 15.5 Cronograma y acciones

- Mostrar fecha planeada y real en sus celdas correctas.
- Deduplicar cuando ambas fechas coinciden.
- Enlazar ambas proyecciones al mismo expediente.
- Mostrar estado con texto y no solo color.
- Conservar contexto, foco y datos ante error.
- Verificar respuestas HTML y JSON del mismo servicio.

## 16. Validación visible

- Navegador de escritorio en cronograma mensual.
- Vista móvil del cronograma sin desbordamiento.
- App del auditor en teléfono real con permiso de ubicación.
- Ejecución anticipada, puntual, atrasada y extraordinaria.
- GPS dentro y fuera de geocerca.
- Medición con precisión mayor a 100 metros y reintento posterior.
- Sucursal sin geocerca.
- Consola sin errores y solicitudes XHR/Fetch correctas.
- Toast, bloqueo de un solo botón, doble toque, foco y conservación de contexto.
- Confirmación de que el Service Worker instalado recibió la versión nueva, si aplica.

La validación local no sustituye la comprobación autenticada en producción después de PR, merge y despliegue seguro.

## 17. Riesgos y mitigaciones

### Doble conteo

Riesgo: mostrar la misma auditoría en fecha planeada y real como dos registros.
Mitigación: proyectar dos marcadores con el mismo id y separar métricas de plan y actividad.

### Compatibilidad de fecha nullable

Riesgo: plantillas, ordenamientos o `__str__` asumen siempre una fecha programada.
Mitigación: inventariar consumidores, agregar pruebas y desplegar migración compatible antes de activar extraordinarias.

### Falsos rechazos de GPS

Riesgo: interiores o mala señal producen precisión superior a 100 metros.
Mitigación: mostrar precisión en vivo, permitir nuevas mediciones y no borrar la captura.

### GPS manipulado

Riesgo: un dispositivo comprometido puede falsificar coordenadas.
Mitigación: no prometer prueba absoluta; combinar distancia calculada, precisión, hora, usuario, fotografías y auditoría.

### Concurrencia

Riesgo: dos auditores intentan cumplir la misma programación.
Mitigación: transacción, bloqueo de fila, validación de estado e idempotencia del servidor.

## 18. Criterios de aceptación

- Se puede construir y consultar el plan completo de un mes por sucursal.
- Ninguna visita se bloquea por ejecutarse antes o después de su fecha programada.
- El auditor selecciona sucursal y programación pendiente de forma explícita.
- La fecha programada permanece fija y la fecha real refleja la ejecución.
- El cronograma muestra ambas fechas sin contar dos auditorías.
- Una visita programada solo se cumple una vez.
- Una visita sin programación puede registrarse como extraordinaria con causa y explicación.
- Las extraordinarias no alteran el cumplimiento del plan.
- El cierre exige precisión GPS de hasta 100 metros y ubicación dentro de la geocerca.
- La distancia se calcula en el servidor y la evidencia queda persistida.
- Un fallo de ubicación no borra el checklist.
- Permisos, rutas directas y concurrencia están protegidos en backend.
- Las métricas distinguen cumplimiento, puntualidad y actividad extraordinaria.
- Las vistas móvil y escritorio se validan en el flujo real.

## 19. No autorización implícita

La aprobación de esta especificación no autoriza por sí sola:

- ejecutar migraciones;
- modificar datos productivos;
- crear PR;
- mergear;
- desplegar;
- configurar o cambiar geocercas.

Cada etapa posterior debe seguir el protocolo del repositorio, comenzar en un worktree limpio desde `origin/main`, ejecutar PostgreSQL aislado, checks, migraciones y pruebas, y obtener las autorizaciones correspondientes para contratos compartidos y producción.
