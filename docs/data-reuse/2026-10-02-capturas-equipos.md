# Ficha de fuentes — protección de capturas de equipos (P3C1)

Fecha y ambientes consultados: 2026-10-02. Catálogo PostgreSQL local aislado y revisión de fuentes de producción de solo lectura realizada por el responsable del hilo.

## Necesidad y unidad de análisis

Proteger un intento de captura directa contra doble clic, reintento concurrente o respuesta perdida. Una captura crea una `OrdenMantenimiento`; un comprobante identifica el intento técnico de un usuario en una operación concreta. El comprobante no constituye trabajo, gasto, factura ni equipo adicional.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros de producción | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo | `activos.Activo` / `activos_activo` | Catálogo de Activos | PK, código, sucursal y vigencia | 196 activos, lectura acotada del hilo | Activos, Mantenimiento, Fallas |
| Trabajo | `activos.OrdenMantenimiento` / `activos_ordenmantenimiento` | Órdenes y capturas directas existentes | PK y folio; ámbito del equipo | 63 órdenes | Bandeja, historial, Activos |
| Seguimiento de trabajo | `activos.BitacoraMantenimiento` | Flujo de la orden | PK y FK orden | Relación existente, inspección de código | Detalle e historial |
| Reporte de falla | `fallas.ReporteFalla` | Reporte y atención de fallas | PK, sucursal, activo opcional | 107 reportes, 33 relacionados con activo | Fallas y Mantenimiento |
| Plan periódico | `activos.PlanMantenimiento` | Planificación de Activos | PK y FK activo | 0 planes | Activos y Mantenimiento |
| Solicitud de falla | `activos.SolicitudFalla` | Flujo de Activos | PK y FK activo | 0 solicitudes | Activos |
| Atención entre fuentes | `mantenimiento.VinculoAtencionEquipo` | Acción explícita P3B | Par único orden/reporte | 0 vínculos | Historial, detalle |
| Auditoría | `core.AuditLog` | `core.audit.log_event` | PK, actor, acción, modelo e ID | Fuente existente, inspección de código | Auditoría interna |

Los conteos representan la fotografía del análisis y no se usan para asignar identidades ni para modificar registros.

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Equipo / activo | Confirmada como referencia de estas capturas | Las tres entradas apuntan a `Activo.pk`; no se infiere identidad por nombre | Ninguna equivalencia nueva |
| Servicio puntual / orden | Confirmada para alcance ACTIVO | La captura existente crea `OrdenMantenimiento`; flota utiliza otro modelo | Conservar diferencias entre recorridos |
| Reporte / orden / plan | Distinta | Son hechos distintos con contratos propios; un vínculo no fusiona ni duplica importes | No se aplica consolidación automática |
| Proveedor / responsable | Distinta | El texto de responsable y el catálogo de proveedores tienen semántica diferente; se conservan los defaults actuales | Sin normalización histórica |

## Decisión de diseño

Reutilizar Orden, Bitácora, Auditoría y las políticas `services_access`. Añadir sólo `ComprobanteCapturaEquipo`: actor nullable (`SET_NULL`), operación concreta, UUID, SHA-256, orden nullable (`SET_NULL`) y fecha técnica. Restricción única por actor/operación/UUID. No guarda descripción, importe, factura, contenido del payload ni otro maestro de equipos.

La transacción abarca comprobante, proveedor de servicio cuando corresponde, orden, bitácora y auditoría CREATE. La restricción UNIQUE serializa dos reintentos concurrentes. Antes de recuperar una orden se vuelve a validar escritura y ámbito del equipo, y la orden recuperada debe seguir autorizada. Misma clave y contenido distinto produce 409; una orden eliminada deja su comprobante y produce 410, sin resurrección.

Los reintentos API de equipos usan la política existente `can_view_costs`: cuando devuelve falso, la respuesta 200 omite `costo_repuestos`, `costo_mano_obra`, `costo_otros` y `costo_total`, incluso si un gestor corrigió esos costos después del alta. Recuperar el intento no concede visibilidad financiera. Primera alta y GET/detalle conservan sus contratos históricos.

Se distinguen las operaciones `orden_pwa`, `servicio_pwa` y `servicio_web`. Primera respuesta API 201; reintento confirmado 200 con la misma orden y el mismo contrato de respuesta. Los clientes históricos que omiten UUID siguen funcionando, sin garantía de idempotencia. Flota e instalaciones no crean comprobantes y conservan sus fuentes y estados existentes.

La huella usa entrada validada/canónica, importes normalizados y bytes SHA-256 de archivos preservando la posición del stream. Los defaults del servidor basados en la fecha actual no entran en la huella del intento. La foto de Registrar equipo conserva la semántica existente: su nombre se registra en comentario, no se añade un nuevo archivo de evidencia. La factura web sigue almacenándose; el intento fallido elimina únicamente archivos nuevos que ese intento guardó, sin tocar archivos confirmados.

La PWA conserva borrador, UUID y snapshot del envío durante el error/reintento. El formulario web usa `data-async-action`; la ampliación opt-in `data-capture-snapshot` de `erp_actions.js` conserva el FormData original ante incertidumbre. Errores conocidos 400/403/404 permiten corregir con la misma UUID; red, 5xx, 409/410 o respuesta desconocida conservan snapshot y bloqueo. Éxito web se informa inline; sólo Nueva captura rota la clave. El opt-in `data-action-inline-feedback` evita un toast duplicado que tape Guardar; los formularios fuera del alcance equipo conservan sus notificaciones y navegación existentes. El fallback HTML conserva UUID/datos y muestra el error; los archivos de un POST nativo rechazado deben seleccionarse otra vez por restricción del navegador. SW y registro versionados juntos.

Consultas reproducibles: `manage.py inventario_fuentes_datos --term equipo --term orden --term mantenimiento` con PostgreSQL aislado configurado. El resultado contiene 30 candidatos léxicos (15 mostrados) y no acredita identidad semántica. El grafo de código disponible carecía de cobertura útil para este dominio; se inspeccionaron los modelos/creadores/consumidores con búsqueda textual y lectura directa como fallback.

Riesgos y pendientes: los borradores y archivos se conservan en memoria durante la sesión abierta, no sobreviven al cierre/recarga del navegador. La garantía exige que el cliente envíe UUID. No se deduplican órdenes históricas ni trabajos legítimos enviados con claves diferentes. La validación visual y el despliegue de producción corresponden al cierre del hilo responsable; una prueba local no sustituye esos pasos.

## Límite de permisos legado detectado — pendiente separado

`core.access.get_effective_module_access` promueve permisos de submódulos al módulo padre cuando el acceso base es `none`. Por ello un usuario con sólo `mantenimiento.app=manage` actualmente puede satisfacer `can_view_costs`; no es correcto describirlo como operador sin acceso financiero. Esa política compartida permanece intacta en P3C1. La prueba del filtro utiliza un actor real del grupo `mantenimiento` con acceso explícito `mantenimiento=view`: puede escribir por su grupo y `can_view_costs` devuelve falso.

Las primeras altas y los GET/detalle históricos de estas APIs conservan la exposición de costos previa a P3C1. Revisar su política financiera y la promoción de permisos constituye un pendiente independiente, con aprobación y validación de sus consumidores; esta entrega no amplía ni redefine permisos ni modifica esos endpoints.
