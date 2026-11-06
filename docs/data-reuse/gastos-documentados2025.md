# Ficha de fuentes — Correcciones documentadas de gastos2025

Fecha y ambiente consultado:6noviembre2026; producción en lectura y PostgreSQL16 local aislado para pruebas.

## Necesidad y unidad de análisis

Una fila de gasto representa un concepto o agregado por centro, periodo y tipo de dato, identificado por `external_key`. Los importadores no deben deshacer una corrección documentada ni recrear sus componentes duplicados. El reporte debe conservar gastos confirmados de2025 cuando la sucursal no tiene ventas, sin crear ventas ni snapshots.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Gasto mensual | `reportes.GastoOperativoMensual` | Importadores Gastos/GastosSucursal y captura existente | ID y external_key; centro/mes/tipo |531 registros2025 contrastados con1194 filas de export | `leer_gastos_mensuales`, rentabilidad |
| Corrección documentada | `core.AuditLog` | Aplicador conciliado, transacción única | action=clasificar_gastos2025_documentados,model=reportes.GastoOperativoMensual,object_id=2025 | Protección sólo tras auditoría confirmada; claves originales y resultantes en payload | Ambos importadores existentes |
| Gasto apto para reporte | Resultado de `leer_costos_mensuales` | Servicios financieros existentes | sucursal_id, estado COMPLETO, monto_mensual, periodo | Simulación12meses; gastos sin ventas enero/febrero | `registros_del_mes`, dashboard, resultado empresa |
| Snapshot histórico | `rentabilidad.SucursalRentabilidad` | Flujo existente | sucursal/periodo | Ninguno para2025 en lectura inicial; prueba cubre snapshot existente | Reportes históricos |
| Fuente documental original | Presupuesto ventas diciembre2025 | Libro gerencial, columnas REAL | Hoja/celda/fórmula y SHA256 |676 celdas coincidentes;21 hijos duplicados dentro de16 SUM;85 conceptos divergentes conservados | Conciliación de datos separada del código |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Claves de export y registros consolidados | Confirmada dentro de plan revisado |1194 filas agrupan exactamente531 registros; el agregado aritmético no prueba legitimidad del gasto | Revisión independiente y raíz completada para planv2 |
| Padre SUM e hijo de adquisición/mantenimiento | Confirmada sólo con misma hoja/mes/celda REAL |21 duplicados163449.72; conservar padre completo porque export omite otros hijos | Aplicación separada tras protección desplegada |
| Coincidencia sólo PRESUPUESTADO | No resuelta |35 filas; no acredita origen ni autoriza sustituir REAL | Mantener dato y conflicto documental |
| Sucursal sin ventas y sucursal sin gastos | Distintas | Túnel enero/febrero tiene gasto documental y ventas ausentes | No inferir fecha de apertura ni actividad |

## Decisión de diseño

Reutilizar AuditLog, sin modelo ni tabla nueva. Los dos importadores validan el archivo completo antes de bootstrap/escritura dentro de atomic y del mismo bloqueo transaccional que el aplicador. Auditoría malformada falla cerrada. El control cubre las claves explícitas documentadas; no es un deduplicador general de archivos renombrados o sin clave.

Extender únicamente el perímetro en memoria del reporte2025 con sucursales que tengan gasto COMPLETO no cero. Unir contra IDs existentes evita duplicar snapshots. No persistir snapshots ni ampliar2026. La UI distingue gasto sin ventas de falta de gasto; margen y punto de equilibrio siguen no calculables. No hay service worker del módulo rentabilidad.

Consultas reproducibles: `manage.py inventario_fuentes_datos --term gasto`; lecturas acotadas de531 gastos, categorías y reglas; verificación de hash del export; comparación de16 versiones del libro; simulación READ ONLY de24 meses del lector y12 meses del resultado empresa. Evidencia externa bajo cierre-integral/continuacion2025-gastos-20261106; contiene fuente por celda, plan, revisiones y previews sin credenciales.

Riesgos y pendientes: el estado COMPLETO mantiene su significado gerencial existente; no certifica pago ni devengo fiscal. Las85 divergencias no se corrigen por inferencia. El código no aplica por sí mismo el plan de datos. La aplicación requiere respaldo, revisión raíz vigente, hashes de preview y módulos desplegados, invariantes e idempotencia, y verificación en pantalla. Gastos2026, categorías, reglas y centros permanecen sin cambios.
