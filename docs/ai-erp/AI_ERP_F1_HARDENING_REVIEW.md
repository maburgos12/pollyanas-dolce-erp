# ERP AI Agent — revisión del corte F1: permisos y recuperación

Fecha: 2026-10-05. Base inspeccionada: `a783f6d120960ddbdd20ce6e74e4d82882b70f99`.

## Estado y autorización

Mauricio autorizó corregir los controles de cuentas desactivadas y completar el
respaldo/restauración de archivos, sin cambiar roles ni cron, con pruebas y
revisión antes del despliegue. Este corte prepara esos prerrequisitos; no habilita
el Agent Core ni incorpora herramientas transaccionales.

**Pendiente de revisión. No mergear ni desplegar el respaldo sin resolver capacidad.**
Producción se inspeccionó en solo lectura; no se modificaron datos operativos,
permisos, variables, cron, retención ni servicios de producción.

## Problema y corrección

### Cuentas desactivadas

Tres funciones compartidas comprobaban autenticación pero omitían `is_active`:

- `activos.services_pasaporte.activos_autorizados`.
- `activos.services_pasaporte.puede_reportar_activo`.
- `mantenimiento.services_access.can_write_mantenimiento`.

Una cuenta desactivada que conservaba sucursal, grupo o privilegios podía seguir
obteniendo autorización si un consumidor pasaba directamente su objeto `User`.
El backend de sesión ya rechaza usuarios inactivos; eso no sustituye la validación
en el servicio compartido que consumiría el futuro agente.

Se añade la comprobación en esas tres funciones. Los roles, privilegios del
personal activo y contratos de sus consumidores se conservan. Se trazaron los
consumidores de pasaportes, captura de fallas, mantenimiento, vínculos y destinos
documentales de compras; las pruebas cubren sus flujos existentes.

### Archivos recuperables

El respaldo existente incluía PostgreSQL y dos directorios de evidencias privadas,
pero omitía `storage/media`, donde se guardan archivos referenciados por modelos
del ERP. Restaurar solamente SQL no recupera fotos ni facturas.

Se extiende `scripts/backup_db.sh` con un cuarto contenido: `media.tar.gz`.
Reutiliza `tar`, staging privado, exclusión mutua, manifiesto SHA-256 publicado al
final, exportación mediante hard links y rotación por conjuntos verificados.

- `MEDIA_ROOT` usa por defecto `storage/media`, ruta de producción verificada.
- Si la fuente no existe, no es directorio o falla el archivo, el comando falla
  y conserva los puntos de recuperación anteriores.
- La verificación, colisiones, publicación, exportación y rotación incluyen media.
- Los manifiestos históricos de dos o tres contenidos siguen siendo válidos.
- SQL acompañado por media/evidencia sin manifiesto no cuenta como SQL legado.
- No se añaden dependencias ni se cambia la política de retención: el script
  mantiene su valor por defecto y el cron conserva su configuración actual.

## Alcance

| Archivo | Cambio |
| --- | --- |
| `activos/services_pasaporte.py` | Rechazo de cuentas desactivadas |
| `activos/tests_pasaporte.py` | Regresión con sucursal, mantenimiento y DG |
| `mantenimiento/services_access.py` | Rechazo de escritura de cuentas desactivadas |
| `mantenimiento/tests_v2.py` | Regresión por grupo, permiso explícito y administrador |
| `scripts/backup_db.sh` | Respaldo completo de media dentro del mecanismo existente |
| `scripts/tests/test_backup_db.py` | Recuperación, fallos, compatibilidad, exportación y rotación |
| Este documento | Evidencias, límites y condiciones de publicación |

No hay modelos, migraciones, endpoints, cambios de interfaz, roles o configuración.

## Validación local

Primero se reprodujeron las omisiones de permisos y media con pruebas que fallaban
contra la base original. Con las correcciones:

- **124 pruebas Django:** `activos.tests_pasaporte`, `mantenimiento.tests_v2`,
  `operacion.tests_fallas_api`.
- **104 pruebas Django de consumidores:** capturas de equipos, planes, órdenes
  desde reportes, vínculos, proveedores, documentos financieros y destinos
  documentales de compras.
- **24 pruebas independientes del respaldo:** restauración de archivos anidados
  con espacios, fallos de fuente/archivo, integridad, concurrencia, compatibilidad
  histórica, exportación restringida y rotación.
- `check`, `migrate --check`, `makemigrations --check --dry-run`, sintaxis Bash y
  `git diff --check`: sin errores ni migraciones nuevas.

Restauración integrada: el script real ejecutó `pg_dump` contra PostgreSQL 16
aislado y migrado. Se restauró el SQL en otra base local y se extrajo media.
La foto de un reporte y el PDF de una orden, ambos sintéticos, conservaron sus
bytes SHA-256 y las rutas almacenadas en la base. El manifiesto verifica los
cuatro contenidos. No se copiaron datos de producción para esta prueba.

Navegador local: usuario activo con equipo de su sucursal accede; UUID de otra
sucursal responde 404; desactivar la cuenta con sesión abierta redirige al login;
volver a iniciar sesión devuelve credenciales inválidas. Sin errores de consola.
Se verificaron solicitudes y respuestas locales; no es prueba de producción.

## Condición de capacidad antes del despliegue

Medición de producción, sin crear un archivo en el servidor:

| Medición | Bytes |
| --- | ---: |
| Media comprimido con `tar`/gzip | 5,421,502,946 |
| Espacio libre del filesystem de respaldos | 18,365,046,784 |
| Tres archivos media del tamaño observado | 16,264,508,838 |
| Tres retenidos más el siguiente en staging | 21,686,011,784 |

El tamaño medido es aproximadamente **5.42 GB por archivo media**. La cuarta copia
temporal ya supera el espacio libre observado, antes de SQL, otros contenidos y
crecimiento. La transición y el estado estable necesitan margen adicional;
no es seguro desplegar con esa capacidad. No se reduce retención ni se cambia cron.

Recomendación: resolver primero almacenamiento suficiente para staging y retención,
con reserva operativa; si se utiliza el NAS existente, verificar recepción e
integridad real y evitar mantener en el VPS todas las copias completas de media.
Eso requiere una propuesta y autorización propias. Los hard links de exportación
no duplican el contenido local, pero tampoco demuestran que el NAS recibió datos.

## Límites de recuperación

- SQL y archivos se capturan secuencialmente. Un manifiesto válido prueba integridad,
  no una instantánea transaccional conjunta si media se modifica o elimina durante
  el respaldo. La recuperación operacional exige ventana consistente o snapshots
  coordinados; no se promete recuperación puntual a partir de esta prueba sintética.
- Los conjuntos anteriores no adquieren retroactivamente los archivos omitidos.
- No se verificó una restauración de producción ni recepción nueva en el NAS.
- No se prueban aún tool calling, confirmaciones, idempotencia o workflows del
  agente: esas capacidades no se implementan en este corte.
- El futuro agente debe revalidar usuario y alcance en cada ejecución y convertir
  los datos a DTO explícitos; no debe enviar el objeto ORM del pasaporte al modelo.

## Rollback y siguiente decisión

Rollback de código: revertir este cambio por Git y desplegar mediante el flujo
oficial, previa autorización. No hay rollback de esquema ni de datos porque no se
modifican. Conservar cualquier conjunto nuevo y su manifiesto: revertir el script
no requiere borrar archivos recuperables.

Los controles de cuentas y el respaldo se revisan por separado dentro del mismo
corte. Antes de publicar: aprobar el diff, resolver almacenamiento y consistencia,
completar CI, desplegar oficialmente y verificar la respuesta visible y el siguiente
respaldo completo. **No activar el agente antes de satisfacer sus gates posteriores.**

## Recursos temporales y entrega

Propietario: Codex. Proyecto local: `erp_ai_f1_hardening_20261005`.
Contenedor: `erp-ai-f1-hardening-20261005-db`.
Volumen: `erp_ai_f1_hardening_20261005_pgdata`.
Puertos reservados: PostgreSQL `127.0.0.1:55487`, preview `127.0.0.1:8127`.

Evidencias privadas y respaldo sintético recuperable:
`~/.codex/task-artifacts/ai-erp-f1-controles-respaldo-20261005/`.
El registro `resources.json` y la evidencia de limpieza documentan su estado;
no hay recursos compartidos ni datos operativos únicos en este entorno.
La rama y el worktree se conservan para revisión mediante el ciclo de vida del
proyecto; no se declaran mergeados ni desplegados.
