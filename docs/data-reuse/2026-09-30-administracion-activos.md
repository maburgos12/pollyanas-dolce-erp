# Ficha de fuentes — Administración y Activos

Fecha: 2026-09-30. Ambiente: PostgreSQL 16.12 en producción, consultas acotadas dentro de una transacción `READ ONLY`; código base `2201e2f8`. La primera entrega modifica presentación y nombres de navegación, sin cambiar modelos, permisos, cálculos financieros o registros operativos.

## Necesidad y unidad de análisis

Gestionar el equipo físico y consultar su relación con incidencias, mantenimiento, compras y gastos sin crear catálogos paralelos. Un equipo, una incidencia, un trabajo, una factura, un pago y un evento de auditoría son unidades distintas. Sus representaciones relacionadas no deben contarse como varios gastos del mismo hecho.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo físico | `activos.Activo` / `activos_activo` | Activos, altas e importación de bitácora actuales | PK, código y QR únicos; sucursal y ubicación | 185 | Fallas, planes, órdenes, API, pasaporte de Operación, etiquetas y presupuesto |
| Plan preventivo | `activos.PlanMantenimiento` / `activos_planmantenimiento` | Activos; ejecución web/móvil de Mantenimiento | PK, activo, frecuencia y fechas | 0 | Agenda, ejecución y órdenes |
| Trabajo de equipo | `activos.OrdenMantenimiento` / `activos_ordenmantenimiento` | Activos/Mantenimiento | PK/folio, activo y plan opcional | 63 | Historial, pasaporte, API y presupuesto |
| Incidencia operativa | `fallas.ReporteFalla` / `fallas_reportefalla` | Fallas/Operación y atención por Mantenimiento | PK/folio, sucursal, activo opcional, duplicado_de | 107 | Bandejas, historial, pasaporte y presupuesto |
| Solicitud histórica | `activos.SolicitudFalla` / `activos_solicitudfalla` | Flujo de Activos | PK/folio, activo y orden_atencion | 0 | Flujo de atención y registro rápido |
| Proveedor comercial | `maestros.Proveedor` / `maestros_proveedor` | Maestros/Compras | PK; identidad comercial/fiscal por verificar por registro | 124 | Compras, equipos y órdenes |
| Técnico/taller | `mantenimiento.ProveedorServicio` / `mantenimiento_proveedorservicio` | Mantenimiento, alta e importación | PK; no FK al proveedor maestro | 37 | Mantenimiento, Fallas y flota |
| Adquisición/servicio comprado | Modelos departamentales en `compras/models.py` | Área solicita; Compras gestiona; área confirma recepción | Solicitud/item/intento/orden/línea/recepción | Contratos inspeccionados; no se extrajeron registros comerciales en esta consulta | Presupuesto, seguimiento, recepción y avisos |
| Gasto reconocido | `reportes.services_presupuesto_real.PresupuestoRealConsolidacionService` y mapeos existentes | Consolidación/command/task del ERP | Fuente, periodo, rubro y centro | Índice de mantenimiento inspeccionado | Presupuesto y reportes consolidados |
| Flota | Modelos de `logistica` y fuentes actuales de mantenimiento | Logística/Mantenimiento | Unidad y registros de servicio/reparación, reporte origen cuando exista | Modelos y consumidores inspeccionados; conteos no refrescados en esta consulta | Historial y panel ejecutivo |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Equipo/activo/maquinaria | Concepto compartido; identidad física individual no resuelta | Dos unidades iguales son dos activos; nombre/ubicación no son claves únicas | Evidencia física antes de fusionar |
| Proveedor/técnico/taller | Candidata | Coincidencia nominal no identifica persona jurídica; las fuentes tienen capacidades distintas | Identidad fiscal/comercial y autorización de vínculo |
| Reporte/orden/servicio | Distinta, con relación por definir | Puede haber varios trabajos por incidente; mismo día/equipo no prueba duplicidad | Cardinalidad y documentos de origen |
| Equipo/instalación | Distinta | ReporteFalla admite instalación sin activo | No crear equipo ficticio para poder reportar |
| Sucursal/ubicación | Distinta | Sucursal es FK; ubicación es texto y puede describir área | Ámbito y vigencia; conservar históricos |

## Decisión de diseño

Reutilizar maestros, equipos, planes, órdenes, incidencias y componentes actuales. La entrega visual conserva rutas, claves de módulo/submódulo y permisos; elimina la barra local adicional y paneles genéricos que anteceden al trabajo diario. Los filtros, formularios, acciones, documentos y exportaciones existentes permanecen disponibles. No se crea una segunda captura.

Los contratos siguientes requieren diseño concreto y aprobación antes de modificación:

- `ReporteFalla` no referencia directamente `OrdenMantenimiento`; `SolicitudFalla` sí tiene orden_atencion. No sustituir ni eliminar el modelo histórico sin comprobar consumidores.
- El estado web de órdenes permite cualquier estado del catálogo; la API define CERRADA/CANCELADA como terminales. La futura política debe reconciliar ambos caminos.
- `_registrar_plan` es compartido por ejecución web/móvil y escribe plan, orden y bitácora sin una unidad transaccional ni identidad de reintento en los caminos inspeccionados.
- `_build_mant_equipo_index` suma órdenes cerradas y reportes cerrados/resueltos por separado, admite estimados históricos y usa ubicación actual. El pasaporte aplica una selección distinta de órdenes. Estas diferencias no demuestran por sí solas doble conteo monetario.
- Hay alta segura de proveedor en `mantenimiento/services_proveedores.py`, pero otros endpoints e importación usan selección por nombre; el perfil técnico no guarda identidad maestra.
- Compras protege un intento vigente por item, compra por intento y recepción parcial con confirmación del área. Un enlace futuro a equipo/trabajo debe conservar esas reglas.
- La vista del catálogo usa permisos de Inventario; navegación, historial y costos usan otros contratos de acceso. La entrega visual no amplía ninguno.

## Consultas y procedimiento reproducible

1. Confirmar `connection.vendor == 'postgresql'` y conectividad; obtener versión PostgreSQL sin credenciales.
2. Abrir `transaction.atomic()` y ejecutar `SET TRANSACTION READ ONLY` antes del inventario y conteos.
3. Ejecutar `inventario_fuentes_datos --term activo --term equipo --term mantenimiento --term falla --term proveedor --term recepcion --presence --limit 35`.
4. Leer `_meta.db_table` y `objects.count()` únicamente de los siete modelos con conteo en la tabla anterior.
5. Revisar definiciones y consumidores mediante grafo de código; comparar rutas web/API y fuentes financieras.

El inventario encontró 72 modelos candidatos y mostró 35: no es una revisión exhaustiva de los 72. Los candidatos léxicos no son equivalencias de negocio. No se consultaron datos personales ni se aplicaron actualizaciones.

## Riesgos y pendientes

No afirmar que todo el ERP carece de duplicidades tras una mejora visual. Falta revisar identidades y documentos por registro, definir estados/autoridad y conciliar importes por periodo/centro. Datos ausentes permanecen pendientes; NULL no equivale a cero. No reasignar históricos Crucero/Bamoa ni gastos al corregir ubicación actual. Validar consumidores afectados y navegador por paquete; toda corrección histórica exige lote aprobado y trazabilidad.
