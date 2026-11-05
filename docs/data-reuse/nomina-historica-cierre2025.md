# Ficha de fuentes — nómina histórica 2025

Fecha y ambiente: 2026-11-05; PostgreSQL de producción (consulta de solo lectura) y PostgreSQL 16 aislado para pruebas.

## Necesidad y unidad de análisis

Conservar la función de cada empleado en cada línea de nómina y distinguir percepciones salariales de otros pagos. La clasificación histórica no debe cambiar cuando cambia la ficha actual de RRHH. Un préstamo afecta el neto entregado, pero no el costo salarial.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente | Identificador | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Persona | rrhh.Empleado | RRHH; catálogo CONTPAQi para identidad histórica | Número CONTPAQi, NSS; no código Point | Catálogo y respaldo restaurado contrastados; diferencias de Excel registradas en AuditLog 42498 | Nómina, reportes |
| Nómina operativa | rrhh.NominaPeriodo, NominaLinea, NominaConceptoLinea | Importación de lista de raya o captura | Periodo y empleado | Consulta actual: cero periodos y líneas de 2025 | Rentabilidad y planeación de personal |
| Documento fiscal | sat_client.CfdiDescargado | SAT | UUID | 1764 recibos recuperados para el contraste histórico | Conciliación fiscal; no sumar además de la nómina |
| Puesto histórico | Histórico CONTPAQi restaurado | CONTPAQi | Empleado y periodo | Exportaciones conservadas con UUID y periodo; no asumir el puesto vigente | Clasificación histórica |
| Soporte de importación | rrhh.NominaImportacion | Importador | Hash y periodo | Modelo existente con resumen | Trazabilidad de la carga |

## Alias y equivalencias

Número CONTPAQi y número Point son identificadores distintos. Los ceros iniciales no cambian el valor numérico del código CONTPAQi; se corrobora identidad con NSS. Un nombre parecido no autoriza un vínculo. Las equivalencias Point pendientes no se aplican en este cambio.

## Decisión de diseño

Extender NominaLinea, sin crear otro maestro ni otra tabla de nómina: guardar puesto y nivel históricos junto a los snapshots existentes. El origen HISTORICA impide completar datos desconocidos con la ficha actual, incluido su salario. La captura explícita de snapshots actuales excluye ese origen. Las líneas legadas sin nuevos snapshots conservan su comportamiento previo; no se reconstruye historia con datos actuales.

Agregar otros_pagos y el tipo de concepto OTRO_PAGO para conservar el neto sin incrementar percepciones ni costo salarial. El lector de rentabilidad sigue sumando únicamente percepciones, con su protección existente contra duplicar despensa. El importador de lista de raya guarda los nuevos snapshots en sus inserciones por lote. La API expone otros_pagos como solo lectura.

Procedimiento: inventario_fuentes_datos --term nomina en PostgreSQL; inspección del importador y lectores; consulta acotada de conteos de 2025; pruebas de inmutabilidad histórica, preservación de faltantes, neto con préstamo, importación y consumidores de rentabilidad.

## Límites y verificación

Estas migraciones no cargan nómina, no cambian fichas ni importes existentes y no resuelven por sí mismas sucursales históricas. La carga financiera requiere comprobación por UUID/periodo/concepto y correcciones documentadas; nunca convertir los otros pagos en gasto ni duplicar el documento fiscal como segunda fuente.
