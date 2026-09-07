# Conteos físicos de sucursales

Diseño aprobado por Mauricio en la conversación del 7 de septiembre de 2026: captura y revisión en app/ERP; ajuste separado. No se autorizan movimientos Point, descuentos de merma, recálculos de consumos, nómina ni modificaciones de maestros por este flujo.

## Contrato operativo

Conteo por sucursal, fecha, responsable y lista explícita de artículos. Los artículos pertenecen al catálogo Point (producto) o al maestro canónico de insumos con código Point. Identidad, nombre, unidad y justificación de unidad se congelan al preparar; el responsable de preparar confirma la unidad de conteo. No hay conversiones implícitas. Un campo vacío es pendiente; cero es un hecho capturado. Se permite incidencia justificada para artículo no contable y hallazgos generales para artículos fuera del alcance.

La asignación autoriza solo ese conteo para el responsable en su sucursal vigente. Para otras sucursales se exige una concesión explícita propia del módulo. Los coordinadores usan el permiso existente de gestión del submódulo de inventario; revisores adicionales tienen acceso específico por sucursal. Permisos reevaluados en cada petición, incluido histórico, descarga y exportación. No se conceden roles de inventario a usuarios de Mermas.

Estados: CAPTURA -> ENVIADO -> ACEPTADO; ENVIADO -> RECONTEO -> ENVIADO. Cancelación con motivo. Aceptar valida la observación física, no modifica stock ni certifica conciliación. Reconteo crea ronda solo para artículos elegidos y conserva las anteriores. Cada transición requiere versión esperada y UUID de operación; el mismo UUID y contenido devuelve la respuesta original, contenido diferente es conflicto. Escritura, auditoría y recibo son atómicos. El cierre antiguo nunca se invoca.

## Referencia Point

Solo lecturas pasivas. La referencia congela un único ciclo exitoso de inventario y sucursal exacta; cantidades ausentes/ambiguas/unidades incompatibles son None, nunca cero. El timestamp de extracción NO es un corte atómico. Se muestra como referencia informativa, pendiente de conciliación temporal; no se promete reconstrucción de movimientos inexistente. Una validación de corte por revisor requiere declaración explícita con motivo/evidencia y fuente completa compatible. La captura y aceptación física no dependen de Point. No se inicia sincronización desde cada teléfono.

## Persistencia y recuperación

Tablas nuevas bajo inventario: cabecera, alcance por artículo, lecturas por ronda, eventos inmutables, recibos idempotentes y evidencias protegidas. FKs PROTECT a fuentes. No seeds operativos ni migraciones destructivas. Borrador por usuario/conteo/ronda/versión; envío online explícito, nunca automático al recuperar sesión. Conflictos no se resuelven sobrescribiendo. Datos HTML/API y evidencias no se cachean en PWA. Mostrar estado de guardado confirmado y advertencia de almacenamiento local fallido. No registrar cantidades esperadas ni rondas previas en HTML/JSON de captura ciega.

## UI

App -> Conteos físicos, lista de asignaciones, captura buscable con avance, incidencias/hallazgos, revisión antes de envío, reconteo y consulta de folio. ERP -> Conteos de sucursales, preparación, filtros, revisión, referencia Point, aceptación con motivo, cancelación, evidencias e historial/exportación. Colores y tipografía del ERP. Acciones data-async-action y toast compartido, botones individuales ocupados, captura conservada, anclas estables. No introducir framework JS. El módulo usa CSS/JS propio para borradores y captura, sin modificar comportamiento de otras pantallas.

## Entrega y verificación

Pruebas PostgreSQL de versión/duplicidad, aislamiento de otras tablas, permisos y revocación, cero/vacío, cantidades inválidas, roundtrip de reconteo, error a mitad de transacción, referencias parciales, unidades, descarga y Excel. Navegador móvil/escritorio con consola y Network, fallos de red y recuperación. Regresión operacion/mermas/core. Migración aditiva inspeccionada y verificada. PR borrador, revisión, CI, merge y deploy oficial solo con validaciones aprobadas. Validación productiva técnica sin inventar conteos reales; piloto físico requiere captura real del personal. Hasta esa evidencia no declarar piloto real completado.

## Límites explícitos

Las futuras integraciones de consumo en compras, logística, reportes y finanzas son incrementales: este módulo ofrece consulta/exportación con fuente y estado; no cambia decisiones de esos módulos. Automatizar ajustes y reconstruir cortes de movimientos en venta requieren trabajo posterior independiente. No borrar conteos para revertir una entrega; deshabilitar nuevas preparaciones y conservar historial.
