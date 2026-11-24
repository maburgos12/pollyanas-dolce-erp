# Logística: causas y controles de la regresión del 24 de noviembre

## Alcance y evidencia

Investigación autorizada por Mauricio el 24 de noviembre. Sin modificar Point,
capturas, entregas ni usuarios. Comparación de Git y PR, reproducciones sintéticas
en PostgreSQL 16 y Node, expedientes de Software Factory y lectura acotada de
Nginx/journal. No se presenta un doble de red o DOM como prueba de teléfono físico.

## Cambios causales

| Síntoma | Cambio exacto | Mecanismo y corrección existente |
| --- | --- | --- |
| Botón ausente al capturar o con ruta iniciada | `3e784e0221143c75026d505994b21d6cb5614152`, PR #1672, 7/nov | El render nuevo exigía `!state.cargaSucursal.paradaId` y ruta PLANEADA. El soporte de recarga del 9/nov conservó la exclusión mientras había sucursal seleccionada. `b8324cad` (PR #1799) retiró esos límites indebidos, manteniendo titular y tramo. |
| Carga enviada presentada como pendiente | Mismo `3e784e02`, función nueva `estado_sync_carga_repartidor` | Consultaba solicitudes sin despacho y devolvía PENDIENTE_ENVIO antes de valorar la carga enviada existente. El caso mixto fallaba; no era que Point careciera de toda la carga. Corregido en `2da754e8` y `a469517b`, integrados por PR #1813. |
| ERP falla si Point está ocupado | `3e784e02`, cambio en `OpenTransferSyncService` | Añadió candado por cuenta con `wait=False` al servicio compartido, mientras el ERP continuaba llamándolo dentro de su petición HTTP. Ante otra sincronización, el servicio rechazaba la consulta; faltaba la cola durable compartida. Corregido por `b51338ac`, PR #1812; se conserva la exclusión de sesiones Point. |
| ERP permanece en curso aunque el worker terminó | `b51338ac585419805bd60c21a6b6a73f89343e01`, PR #1812, 24/nov | El consumidor nuevo consulta `/logistica/rutas/.../?carga_point_estado=...`. La política cache-first genérica del SW, presente desde `c4ef05a5` (28/abr), podía devolver siempre el primer202. La opción fetch no-store no era respetada por el SW. El API `/api/` ya tenía otra política, por eso no bastaba probar el API. Corregido por `a79f8957`, PR #1813. |

La fecha de introducción del código no demuestra la fecha del primer incidente.
Los casos mixtos y con otra sincronización podían funcionar durante semanas cuando
no se presentaba la combinación que activa el defecto. El ERP y la app tampoco
tenían originalmente el mismo camino de ejecución.

## Por qué las reglas no lo detuvieron

- PR #1672 tenía CI aprobado, pero sus pruebas nuevas usaban una transferencia
  enviada o una pendiente por separado. Faltaba enviada + pendiente en la misma
  ruta. Las pruebas del render comprobaban texto/versiones y no ejecutaban la
  disponibilidad del botón dentro de una captura.
- PR #1812 también tenía todos los checks aprobados. La prueba Django del polling
  no atraviesa un service worker. Los escenarios Chrome/WebKit registrados en su
  expediente ejecutaban consumidores con respuestas simuladas, sin el SW real en
  medio. No probaban el cruce que cacheaba202.
- Los gates de Factory comprueban revisión, salida, SHA e integridad de evidencia.
  No deducen que los escenarios sean suficientes ni convierten una comprobación
  parcial en validación de todas las cuentas y teléfonos.
- El monitor de trabajos ve el estado del servidor; una interfaz detenida con
  trabajo SUCCESS puede quedar fuera de ese síntoma. Una alerta es detección,
  no una reparación ni una prueba de que el usuario pudo guardar.

No se encontró evidencia de que se hubieran desactivado los checks de esos PR:
los defectos pasaron con cobertura insuficiente, no por un gate fallido ignorado.

## Reproducción y prevención añadida

`node scripts/tests/logistica_point_consumer.cjs` ejecuta juntos el JS real del
ERP y el SW real con DOM/red/reloj aislados. Precarga202 y exige que llegue200,
vuelva a habilitar el botón, no lea/escriba caché privada, conserve filtro y
borrador y recupere una desconexión. Además ejecuta el render real de la PWA con
sucursal seleccionada para PLANEADA, EN_RUTA sin recarga, EN_RUTA con recarga,
ruta completada, preview y otro titular. Son nueve escenarios; no son navegador
ni teléfono físico. Está conectado al job obligatorio checks; su fallo bloquea
el agregado test, que requiere checks exitoso. No cambia permisos de GitHub.

Reproducciones históricas, sin reemplazar archivos operativos:

1. Mismo consumidor actual con SW de `b51338ac`: falla porque conserva202 y no
   alcanza ACTUALIZADA. Con SW actual, pasa.
2. Mismo ejecutor PWA con template de `3e784e02`: falla porque no ofrece el botón
   con sucursal seleccionada. Con template actual, pasa.
3. Función histórica `estado_sync_carga_repartidor` de `3e784e02`, cargada sólo en
   memoria de una prueba PostgreSQL aislada: la regresión de enviada + pendiente
   falla con PENDIENTE_ENVIO frente a ACTUALIZADA esperada. Función actual pasa.

Las regresiones Django de carga disponible, cola, alertas y alcance siguen en el
runner completo existente. Se añade comparación de la fila completa ParadaRuta
entregada al caso de actualización/captura que ya protege la línea y EntregaRuta:
no basta comprobar una tabla de entregas si la operación guarda el hecho en parada.

## Incidente502 distinto

La captura del usuario marca 24/nov18:48:58UTC (11:48:58 Sinaloa). En ese segundo,
Nginx registró reset del upstream `127.0.0.1:8011` en POST `/logistica/rutas/`.
Journal muestra recreación de web18:48:17–18; el expediente previo registra TERM
11:48:05 y escucha restablecida11:49:16, producción `56400d4e` (PR #1811,
ventas-contexto-contingencia), no PR #1812 de carga aún sin publicar.

El despliegue reinicia/recrea web y comprueba readiness después; eso detecta que
regresó, pero no garantiza disponibilidad continua durante el reinicio. Esta
investigación no atribuye el502 al resultado PENDIENTE_ENVIO, a una captura ni al
teléfono. Tampoco afirma haber convertido el despliegue en uno sin interrupción:
ese cambio requiere resolver el relevo del servidor antes de retirar el anterior,
con su propia aceptación y prueba de tráfico continuo. Los controles nuevos de
este PR impiden las regresiones reproducidas del botón; no prueban disponibilidad
ininterrumpida del despliegue ni sustituyen pruebas en los teléfonos pendientes.

## Evidencia y recursos

Expediente privado de tarea `logistica-auditoria-regresion-20261124`, registrado
por el ciclo oficial y Factory. Contiene logs RED históricos, pruebas actuales,
CI y fuente del incidente502. Datos locales exclusivamente sintéticos; proyecto
Compose exclusivo `erp_logistica_audit_20261124`, puerto64734. La entrega registra
su limpieza o responsable antes de retirar recursos. Ninguna consulta de esta
investigación pulsó botones de producción ni generó capturas/avisos de prueba.
