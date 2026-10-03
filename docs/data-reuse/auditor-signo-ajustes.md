# Ficha de fuentes — efecto firmado de ajustes del historial

Fecha y ambiente consultado: 2026-10-02; PostgreSQL de producción (lectura acotada) y pruebas locales aisladas.

## Necesidad y unidad de análisis

Calcular el efecto de un ajuste sobre el saldo auditado. Una fila representa un movimiento Point, no una nueva declaración de ajuste ni una merma.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Historial importado | PointProductHistoryImport / pos_bridge_pointproducthistoryimport | AuditStockHistoryService.capture | Hash canónico por sucursal/producto Point | Importación existente con cobertura temporal explícita | Auditor y conciliación materializada |
| Efecto del movimiento | PointProductHistoryRow / pos_bridge_pointproducthistoryrow | Misma captura canónica | Importación + row_number (FK_Movimiento) | Lectura mensual: ajustes de entrada y salida conservados, con existencia anterior/nueva | reconcile, reconcile_many, capture |
| Saldo calculado | ProductInventoryAuditCase / reportes_productinventoryauditcase | InventoryAuditMaterializer | Mes + sucursal + producto | Proyección existente, no stock operativo | Agente auditor y Producido vs Vendido |
| Ajuste operativo | AjusteInventario / inventario_ajusteinventario | Flujo de autorización operativo | Folio | Candidato léxico del inventario de fuentes; no utilizado ni modificado | Inventario operativo |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Cantidad Point y efecto firmado | Distinta | Una salida puede reportar cantidad positiva; el efecto lo acredita existencia nueva menos anterior | Si magnitud o dirección contradicen la secuencia, conservar movimiento desconocido |
| Ajuste y merma | Distinta | Son categorías existentes diferentes | No convertir ajustes a mermas |

## Decisión de diseño

Extender únicamente el cálculo compartido existente: para ajustes usar la diferencia de existencias, comprobar su magnitud contra el valor absoluto de cantidad y comprobar la dirección explícita ENTRADA/SALIDA. Una cantidad ya negativa no se invierte de nuevo. Contradicciones usan unknown_movement_ids, que impide materializar como conciliado. Se conservan cobertura, FK, datos originales, aprobación documental y conteo físico separados.

No crear tablas, importaciones paralelas, equivalencias de producto, formularios ni ajustes operativos. Consumidores afectados: conciliación individual, lotes, materializador y agente; su contrato de retorno no cambia.

Procedimiento reproducible: `manage.py inventario_fuentes_datos --term ajuste --term historial --term inventario`; lectura mensual acotada de tipos y diferencias de existencias; pruebas del servicio y consumidores sobre PostgreSQL. Actualizar cobertura únicamente si falta, mediante capture sin force y sesión protegida; una segunda captura COMPLETE no realiza HTTP.

Riesgos y pendientes: un saldo aritmético cero no acredita origen físico, trazabilidad documental ni autorización de cierre. Los movimientos contradictorios no se compensan ni se corrigen en la fuente.
