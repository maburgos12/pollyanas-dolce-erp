# Ficha de fuentes — Gafetes verificables

Fecha y ambiente consultado: 2026-10-09; metadatos en PostgreSQL local aislado, cobertura en producción con consulta agregada de solo lectura.

## Necesidad y unidad de análisis
Un colaborador de RRHH conserva un QR aleatorio de credencial vigente. La consulta pública muestra exclusivamente nombre, código existente, fecha de ingreso y vigencia actual. El QR no autentica en el ERP ni concede descuentos o permisos.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Identidad y relación laboral | rrhh.Empleado / rrhh_empleado | Alta de Capital Humano, vacantes e importación de lista de raya | PK; codigo único en RRHH | Producción: 97 registros, 68 activos; ninguno activo sin nombre o fecha, ninguno con baja desde su ingreso actual | Nómina, asistencia, bonos, logística, gafetes |
| Baja | rrhh.EmpleadoBaja / rrhh_empleadobaja | RRHH; save desactiva la ficha vinculada | empleado_id; fecha_baja | Consulta de activos con bajas_rrhh.fecha_baja >= fecha_ingreso: 0 | Nómina, identidad operativa y vigencia |
| Acceso ERP | auth.User / auth_user | Administración de accesos | usuario_erp_id, independiente de estado laboral | Código y contrato inspeccionados; no se consulta información de usuarios | Login y permisos, no fuente de vigencia laboral |
| Token QR | rrhh.Empleado.gafete_token | Default UUID individual, backfill inicial y emisión autorizada | UUID aleatorio único; NULL tras revocación/baja | Nuevo campo; existente segno==1.6.6 genera SVG | Consulta pública y exportación para imprenta |

## Alias y equivalencias

| Términos | Estado | Evidencia | Revisión requerida |
| --- | --- | --- | --- |
| Colaborador / empleado | Confirmada para este alcance | Solicitud y maestro RRHH existente | No se fusionan registros |
| Código de colaborador / token QR | Distinta | codigo identifica la ficha; UUID identifica la liga revocable | Mostrar código y conservar token aleatorio separado |
| Activo laboral / usuario ERP activo | Distinta | Ficha laboral y autenticación son entidades diferentes | La página usa exclusivamente activo laboral |

## Decisión de diseño
Extender Empleado con un solo campo, sin tabla maestra duplicada ni captura nueva. El default también funciona en bulk_create de lista de raya. La baja elimina el token; el reingreso crea uno nuevo. Revocar no altera activo, nómina ni datos personales. Un guardado de una ficha anterior conserva el token vigente de la base bajo bloqueo, para no restaurar un QR revocado. Las altas inactivas y códigos desconocidos no exponen datos públicos.

Procedimiento: `inventario_fuentes_datos --term empleado --term colaborador --term gafete`; consultas agregadas Empleado.count, activo=True, nombre vacío, fecha_ingreso NULL y bajas_rrhh.fecha_baja >= F(fecha_ingreso). El inventario encontró 37 candidatos léxicos; se confirmó semánticamente Empleado mediante el código y contratos. No se exportó información personal para esta ficha.

Pendientes y límites: el QR valida vigencia, no identidad física del portador; el gafete físico debe permitir cotejar al titular. Las fechas se toman de RRHH; no se reinterpretan como antigüedad acumulada ante reingresos. La exportación incluye únicamente colaboradores activos con token vigente, nunca regenera tokens al descargar. No se agrega fotografía a la página pública.
