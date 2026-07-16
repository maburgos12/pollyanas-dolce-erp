# Diseño: PWA de Logística segura con varias ventanas

## Objetivo

Permitir que un repartidor mantenga más de una ventana de la PWA abierta en el mismo iPhone o Android sin perder la sesión, duplicar operaciones, borrar capturas pendientes ni ejecutar procesos automáticos en paralelo.

El trabajo se desarrollará y probará de forma aislada. No se hará merge ni deploy mientras los colaboradores estén operando rutas.

## Hechos comprobados

- Producción usa una sesión Django persistente de 14 días.
- Cada ventana obtiene un JWT de acceso de 5 minutos y uno de renovación de 1 día.
- Los refresh tokens no rotan ni se invalidan después de usarse.
- Cada ventana conserva su JWT en memoria y `sessionStorage`; no comparte ese token con las demás ventanas.
- El logout Django elimina la sesión compartida del navegador. Por eso no sirve para cerrar selectivamente una ventana anterior.
- La PWA actual puede ejecutar `logout(false)` ante un fallo de renovación y borrar colas offline compartidas. Una ventana vieja no debe poder borrar trabajo de otra.
- Safari soporta `BroadcastChannel` y Web Locks desde la versión 15.4. La protección del servidor seguirá siendo obligatoria aunque esas API no estén disponibles.

## Decisión

No se cerrará automáticamente la sesión anterior cuando se abra otra ventana del mismo navegador.

Se implementará una defensa en cuatro capas:

1. Recuperación silenciosa e independiente de autenticación por ventana.
2. Coordinación de eventos entre ventanas, sin compartir credenciales.
3. Exclusión mutua para procesos automáticos.
4. Idempotencia y control de concurrencia en el servidor para toda operación con efectos.

## 1. Autenticación por ventana

Cada ventana conservará sus propios tokens. Los tokens no se escribirán en `localStorage` ni se transmitirán por `BroadcastChannel`.

Ante una respuesta `401`, la ventana ejecutará esta secuencia:

1. Reutilizar una sola promesa de renovación para todas las peticiones concurrentes de esa ventana.
2. Intentar renovar el JWT con su refresh token.
3. Si el refresh expiró, pedir un par nuevo mediante la sesión Django existente.
4. Repetir una sola vez la petición original.
5. Mostrar el login solo si también expiró la sesión Django.

Un fallo automático de autenticación limpiará exclusivamente los tokens de esa ventana. No borrará tracking, capturas, fotografías serializadas ni colas offline.

El botón explícito `Salir` sí cerrará la sesión Django y avisará a todas las ventanas para regresar al login. Si inicia sesión un usuario diferente en el mismo navegador, todas las ventanas deberán recargar su identidad antes de permitir una captura.

## 2. Coordinación entre ventanas

La PWA usará un canal del mismo origen para publicar eventos sin datos sensibles:

- `session-logged-out`
- `identity-changed`
- `route-data-changed`
- `operation-started`
- `operation-completed`
- `operation-conflict`

La ventana receptora invalidará su caché de la ruta y consultará nuevamente al servidor cuando vuelva a estar visible. No copiará el estado JavaScript de otra ventana.

Se usará `BroadcastChannel` cuando exista y el evento `storage` como compatibilidad. La ausencia de ambos no compromete la integridad porque el servidor conserva la protección definitiva.

## 3. Procesos automáticos

Solo una ventana podrá ejecutar cada proceso automático por usuario y ruta:

- tracking GPS periódico;
- vaciado de tracking pendiente;
- reenvío de mutaciones offline;
- sincronización automática de la ruta cuando corresponda.

La ventana debe estar visible y adquirir un Web Lock de corta duración antes de ejecutar. No se mantendrá un lock durante toda la ruta. Cada ciclo adquiere, ejecuta y libera, evitando bloqueos permanentes si Safari suspende o elimina una pestaña.

Cuando Web Locks no esté disponible se usará un lease con propietario, vencimiento corto y renovación en `localStorage`. El backend seguirá deduplicando cada evento automático.

## 4. Operaciones idempotentes

Toda operación con efectos tendrá:

- una clave idempotente estable para el intento;
- una huella del payload;
- una identidad de negocio formada por recurso, acción y versión esperada;
- transacción y bloqueo de las filas de dominio afectadas;
- respuesta original recuperable;
- actor y fecha de la operación aplicada.

La clave del intento protege reintentos por mala señal. La identidad de negocio protege dos clics generados independientemente por ventanas diferentes.

### Respuestas

- Primer intento válido: aplica la operación y devuelve el estado actualizado.
- Repetición del mismo intento y mismo payload: devuelve la respuesta original con `already_applied=true`.
- Otra ventana ya aplicó exactamente el mismo resultado: devuelve el estado actual con `already_applied=true`; no crea otro evento.
- Misma clave con payload distinto: rechaza con conflicto de idempotencia.
- Operación simultánea todavía en proceso: responde `409` y permite consultar el resultado.
- Recurso modificado desde que se abrió la pantalla: responde conflicto de versión y entrega el estado vigente; nunca sobrescribe silenciosamente.

### Mensajes de usuario

- `Ya estaba registrado por {persona} a las {hora}. No se duplicó.`
- `La información cambió en otra ventana. Ya actualizamos esta pantalla.`
- `La operación se está procesando. Espera un momento; no necesitas volver a enviarla.`

Una repetición idéntica se mostrará como información, no como error.

## Cobertura funcional

La primera implementación cubrirá todas las mutaciones de la PWA de Logística:

- iniciar turno;
- liberar/iniciar ruta;
- confirmar una línea o el total de carga;
- registrar recarga CEDIS;
- confirmar entrega;
- registrar desvío y tracking manual;
- cerrar turno;
- finalizar ruta;
- inspecciones, combustible, lavado y reportes creados desde la PWA;
- reenvíos de la cola offline.

La confirmación de entrega ya tiene bloqueo de filas, `client_event_id` y respuesta idempotente. Se conservará ese contrato y se adaptará al mecanismo común sin cambiar las reglas de geocerca, evidencia o revisión administrativa.

## Fuera de alcance

- No cambiar las reglas Point de `Solicitado`, `Enviado` o `Recibido`.
- No cambiar sumatorias ni segmentación de carga.
- No marcar visitas o entregas mediante geocerca.
- No modificar asignaciones de repartidor, acompañantes o unidad.
- No extender este cambio a otros módulos del ERP en esta etapa.
- No desplegar durante la jornada operativa.

## Pruebas obligatorias

### Autenticación

- Dos ventanas con tokens independientes permanecen autenticadas.
- Varias peticiones `401` simultáneas producen una sola renovación por ventana.
- Refresh vencido y sesión Django vigente recuperan la ventana sin login.
- Una ventana vencida no borra colas compartidas.
- Logout explícito cierra todas las ventanas.
- Cambio de usuario invalida la identidad anterior antes de capturar.

### Concurrencia

- Dos clics simultáneos idénticos producen un solo efecto y una sola auditoría.
- Dos payloads distintos para la misma versión producen conflicto, sin sobrescritura.
- Un reenvío offline después del éxito recupera la respuesta original.
- Un reenvío antiguo no revierte información nueva.

### Navegadores

- Safari iPhone con dos pestañas.
- PWA instalada y una pestaña Safari simultáneas.
- Chrome y Samsung Internet en Android.
- Navegador sin `BroadcastChannel` o sin Web Locks.
- Suspender y reactivar una pestaña después de más de 5 minutos y de más de 24 horas.

### Regresión operativa

- Una sola ventana conserva exactamente el flujo actual.
- Point, carga por tramos, entregas con alerta, acompañantes y unidades asignadas mantienen sus invariantes.
- Las colas offline sobreviven a expiración, recarga y cambio de ventana.

## Entrega y activación posterior

El desarrollo se hará en `codex/logistica-multiventana-segura`, sin merge ni deploy inmediato.

Antes de desplegar:

1. Checks y pruebas automatizadas en verde.
2. Simulación local de dos ventanas y de concurrencia real.
3. Revisión del diff para confirmar que no modifica reglas Point, geocerca o cantidades.
4. PR independiente.
5. Deploy fuera de la jornada de rutas.
6. Bump del service worker y `collectstatic`.
7. Validación controlada con una cuenta de prueba y luego con un iPhone.
8. Observación de `401`, `409`, reintentos e idempotencia antes de habilitar el uso general.

El cambio no requerirá que los colaboradores cambien contraseña ni cerrará las sesiones que ya tengan abiertas.
