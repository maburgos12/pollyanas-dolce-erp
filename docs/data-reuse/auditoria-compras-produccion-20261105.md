# Ficha de fuentes — auditoría de compras, insumos y producción

Fecha y ambiente: 05/11/2026; PostgreSQL producción, consultas READ ONLY y código `db1ff9bc78f256dd7b08c4fd94457415ad62ad2a`. No se ejecutaron escritores ni HTTP Point. Esta ficha sigue `docs/data-reuse/ficha-fuentes-template.md`.

## Necesidad y unidad de análisis

Reconstruir la cadena compra→recepción→custodia→consumo→producción, por identidad canónica, ubicación, unidad y periodo, agregando lote solo donde haya evidencia. Separar maestro, documento, evento, snapshot, proyección y evidencia física. Ver fórmulas y cobertura en el plan principal.

## Fuentes candidatas

| Concepto / naturaleza | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
|---|---|---|---|---|---|
| Insumo, maestro | `maestros.Insumo` / `maestros_insumo` | Catálogo ERP y flujos de enlace con Point; autoridad Point en identidad de su dominio | PK ERP, código Point y PK original contextualizado; no unicidad global por nombre | 555 activos; lector resuelve 554 identidades; #433 CAJA CH HERRAMIENTA | Compras, recetas, inventario, mermas, reportes |
| Alias, maestro | `maestros.InsumoAlias` / `maestros_insumoalias` | Curación/importación existente | Alias normalizado y FK | Presencia verificada; no auditados todos sus enlaces | Matching; no prueba histórica independiente |
| Pedido/OC clásicos, documento | `SolicitudCompra`, `OrdenCompra` / `compras_solicitudcompra`, `compras_ordencompra` | Flujo ERP de compras y automatización autorizada | PK/folio, solicitud→orden | 0 registros de ambos | Recepción clásica, planeación |
| Recepción clásica, documento | `RecepcionCompra` / `compras_recepcioncompra` | Recepción ERP | Folio/orden, fecha, estado/conformidad | 0 registros. Código aplica cantidad solicitada, sin líneas de cantidad recibida | Inventario mediante `_apply_recepcion_to_inventario` |
| Compra/recepción departamental | `CompraRealizadaDepartamental`, `RecepcionItemDepartamental` / tablas `compras_*` correspondientes | Captura compra e intento/orden departamental | Compra/item/intento; recepción→línea orden; comprobante | Presencia verificada; 20 recepciones desde 14/09 a 01/11 | Compras departamentales, activos y gastos; no presumir vínculo a insumo |
| Compra Point importada y costo | `maestros.CostoInsumo` / `maestros_costoinsumo`, `raw.source=POINT_COMPRAS_HISTORICAS` | `PointPurchaseExtractionService` y `PointPurchaseCostImportService` | purchase_id/folio, nombre, cantidad/unidad/costo; hash de contenido | Octubre: 257 líneas/130 documentos/98 insumos | Kardex, costo histórico, proyección/finanzas |
| Factura / CFDI | `sat_client.CfdiDescargado` / `sat_client_cfdidescargado` | Descarga SAT existente | UUID, emisor/serie/folio/fecha; conceptos XML | 1,159 emisiones octubre; #40891 candidato para compra 1716279 | Conciliación fiscal/bancaria; evidencia de factura, no recibo de almacén |
| Remisión / recepción física | Documentos fuente de compra, comprobantes y soportes operativos | Proveedor, receptor y flujo autorizado | Documento y línea, receptor, lugar/fecha | En muestra de harina no se verificó remisión firmada ni recepción física | Debe enlazarse a compra/recepción sin duplicar inventario |
| Kardex, evento/representación | `inventario.MovimientoInventario` / `inventario_movimientoinventario` | Importador compra, sincronizador Point, consumo BOM, ajuste/recepción ERP | PK/source_hash, insumo+almacén; tipo, trazabilidad; lote/origen opcionales | 56,521 totales; 10,025 octubre; 0 con lote/origen bitácora | Inventario, auditor consumo, hechos diarios y finanzas |
| Saldo ERP, proyección | `ExistenciaInsumo` / `inventario_existenciainsumo` | Importadores y movimientos/deltas internos | Único insumo+almacén | Presencia verificada; no se considera fuente viva sustituta | Lectores legados, parámetros reorden, auditor consumo |
| Snapshot insumo Point | `PointInsumoInventorySnapshot` / `pos_bridge_insumo_inventory_snapshots` | Captura canónica existente protegida por sesión Point | Único sync_job+branch+insumo; captured_at y raw | 296,076 filas al corte, desde 22/08; lecturas vigentes 352 por ubicación | `CanonicalPointInventoryService`, suministro, proyección, inventario |
| Estado/cobertura captura | `PointSyncJob`; `AlmacenSyncRun` / `inventario_almacensyncrun` | Sincronizadores existentes | Job/ciclo, parámetros, resumen y errores; corrida legacy | 3 AlmacenSyncRun; transferencia muestra refiere baseline_run_id=4, cuya existencia no se infiere del conteo | Lectores, freshness, fronteras y auditoría |
| Transferencia Point | `PointTransferLine` / `pos_bridge_transfer_lines` | `/Transfer/GetTransfer`, persistencia/sync existente | transferencia+detalle, branch origen/destino, hash, versión vigente | 68,209 filas desde 28/03; muestra #66559 20/20 KG; Caja CH tiene versión superada | Movimientos, logística, balance producto y auditor |
| Producción Point | `PointProductionLine` / `pos_bridge_production_lines` | Sincronizador de producción Point | producción+detalle, fecha, unidad, dominio y FK receta/insumo | 14,775 filas; 1,616 octubre; 1 sin receta | Consumo BOM, producido/vendido, hechos diarios |
| BOM / rendimiento | `Receta`, `LineaReceta`, `RecetaCostoVersion` / `recetas_*` | Edición/importación aprobada de recetas y versionado de costos | Receta+línea/posición; unidad, match y rendimiento; versión de costo | Presencia verificada; 10 líneas harina inspeccionadas para detalle 31/10 | Costeo, MRP, suministro, consumo teórico |
| Captura receta Point | `PointRecipeNode/Line` / `pos_bridge_recipe_nodes`, `pos_bridge_recipe_node_lines` | Extracción de recetas Point | run+identity_key/nodo+línea, raw BOM/rendimiento | Presencia verificada; no certificada vigencia histórica en esta fase | Proyección recursiva y vinculación |
| Lote / bitácora, evento físico | `LoteProduccion`, `BitacoraOperativaLinea` / `inventario_loteproduccion`, `operacion_bitacoraoperativalinea` | Flujo de cierre autorizado de bitácora | Lote único; OneToOne línea origen; movimiento FK | 0 lotes y 0 líneas | Trazabilidad de producción; habilitación operacional pendiente |
| Merma Point | `PointWasteLine` / `pos_bridge_waste_lines` | Sync Point | movimiento externo/hash, branch, fecha y unidad | 1,964 filas desde 02/01; no se auditó cada aplicación | Balance/cierre producto y reportes |
| Merma reportada ERP | `MermaInsumo/Event` / `mermas_mermainsumo`, `mermas_mermainsumoevento` | Captura/revisión ERP | request_id, payload_hash, estados, cantidades reportada/aprobada | 680 registros | Flujo de mermas. Estado aprobado no prueba automáticamente aplicación Point |
| Ajuste | `AjusteInventario` / `inventario_ajusteinventario` y movimiento | Solicitud/aprobación/aplicación ERP o baseline importado | Folio/PK, fecha aplicada, movimiento y signo | Presencia verificada; Caja CH ajustes baseline 71 y −15 pza | Inventario; baseline no es consumo |
| Conteo físico | `ConteoFisicoMensual/LineaConteoFisico`; `ConteoSucursal/LecturaConteoSucursal` / `inventario_*` | Captura/reconteo/aceptación | Conteo+línea+ronda+fecha+ubicación | 169 líneas mensual abril BORRADOR; 8 conteos sucursal aceptados, ninguno Almacén/CEDIS | Conciliación física; estados borrador/enviado no sustituyen aceptado |
| Consumo mensual, cálculo | `ConsumoInsumoMensual` / `inventario_consumoinsumomensual` | `ConsumoInsumoAuditService` | mes+insumo, sin ubicación | 2,396 filas marzo–octubre; #2212 harina inspeccionado | Reportes, costo real/P&L; cambio de contrato requiere regresión |
| Órdenes/proyecciones, plan | `ProductionOrder/Line`, `ProyeccionProduccion`, `AutoPurchaseRequestSnapshot` | Planeación y aprobaciones ERP | orden+receta/sucursal, periodo y versión | Órdenes/proyecciones presentes; 0 AutoPurchaseRequestSnapshot y 0 ProductionExecutionLog | Suministro/proyección; no recepción ni producción efectiva |
| Auditor producto, expediente | `ProductInventoryAuditRun/Case/Event` / `reportes_*` | Materializador/agente/reglas y revisión humana existentes | mes+PointBranch+PointProduct, fingerprint, eventos | Código/contratos comprobados; no se repitió conteo de casos en esta fase | Auditor mensual, producido/vendido, exportaciones |

## Alias y equivalencias

| Identificadores / términos | Estado | Evidencia y caso contrario | Revisión requerida |
|---|---|---|---|
| Costo #12971 ↔ movimiento #55722 | Confirmada como representación del mismo documento importado | costo_insumo_id, purchase_id, folio y hash; no recepción física independiente | No nueva equivalencia aplicada |
| Compra 1716279 ↔ CFDI #40891 | Candidata fuerte | Proveedor/folio/importe; fecha 31/10 frente 30/10, 150 KG frente 15 FARDO; sin FK comprobada | Yesenia/Ernesto confirman documento/presentación y entrega |
| 1 kg ↔ 1,000 g | Confirmada dimensional | Entrada #55722 conserva conversión explícita; mismo insumo | No implica convertir fardos, sacos o piezas |
| Transferencia #66559 ↔ #55691/#55692 | Confirmada documental | Hash fuente/derivado, insumo, ubicaciones, 20 KG→20,000 g | No vínculo con lote comprado |
| PROD-23534/23535/23536 ↔ consumos de harina | Confirmada calculada | Hash BOM + suma de componentes actuales; 3/3 | No afirmar consumo físico ni vigencia de BOM |
| CAJA CH #433 ↔ empaque consumible | Candidata de clasificación | HERRAMIENTA y tres referencias de receta; puede existir venta separada | Revisar uso físico/documento, no cambiar maestro |
| #433/código075 ↔ movimiento Point “Rollos Termicos” FK169 | No resuelta/contradictoria | Fila #13243 5 PZA; distinta evidencia frente FK75 CAJA CH 15 PZA #36074 | Ver documento original y vigencia histórica |
| CUARTO_FRIO ↔ CEDIS / ALMACEN_1 ↔ Almacén | Implementada en dominio inventario | Constantes/correspondencia de servicio y par de muestra | No equivale a sububicaciones CFP_1_1/ARMADO/CFP_1 |
| Pan Dawn #508/#560, 311026, 50161509, MOL20/MOL24 y conversiones pendientes | No resueltas conforme al expediente de identidad | Catálogo actual y notas heredadas; no aplicar alias por parecido | Mauricio y responsables según dominio |
| Factura ↔ movimiento / gasto | Distintas representaciones, posible relación | La factura soporta la operación, no es otra recepción/gasto | Conciliación documental antes de enlazar |

## Decisión de diseño

Reutilizar maestros Point/ERP, snapshots, réplicas transaccionales, conversiones dimensionales documentadas, servicios de revisión y bitácoras/lotes/conteos existentes. No crear otro catálogo, capturador de compras o libro de inventario. Separar el resultado de auditoría de las fuentes.

Extender lectores con manifiestos, procedencia y versiones; no reutilizar como autoridad física el agregado mensual ni el fallback de proyección. Recomendar una proyección de expediente de insumo con ubicación/unidad porque los contratos existentes no cubren ese grano (mes+insumo sin ubicación, o producto Point obligatorio). El esquema exacto y su migración se aprueban antes de implementación. No duplicar ProductInventoryAuditCase para productos ya cubiertos.

## Consultas y procedimiento reproducible

`inventario_fuentes_datos --term insumo --term compra --term recepcion --term consumo --term produccion --term suministro --term lote --presence --details --limit 100` ejecutado en el contenedor web existente después de `pg_isready` y comprobación de configuración PostgreSQL. Metadatos en `fuentes-metadatos.json`.

Los scripts `consulta_evidencia.py`, `consulta_muestra.py`, `consulta_conciliacion.py`, `consulta_controles.py` se pasaron por stdin a `python manage.py shell` dentro del contenedor existente, con `PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000'`. Cada script además abre transacción READ ONLY. No se copiaron scripts al VPS. Resultados correspondientes: `evidencia-produccion.json`, `muestra-documental.json`, `conciliacion-muestra.json`, `controles-vigentes.json`.

`verificar_ejemplo.py` ejecuta solo cálculos/asserts locales sobre resultados congelados; no es prueba de nueva implementación ni consulta Point. Fuentes de código citadas por ruta en el plan; revisión local/VPS idéntica al corte. Las consultas no constituyen snapshot transaccional único entre los cuatro archivos: cada extracción identifica su ámbito y la fuente puede avanzar entre ellas.

## Riesgos y pendientes

Sin conteo físico aceptado Almacén/CEDIS para fronteras del ejemplo; sin lotes ni bitácoras; composición histórica por demostrar; equivalencia FARDO→KG no aprobada; compras externas completas y revisiones/cancelaciones no verificadas contra origen. Estados del scheduler no prueban ejecución exitosa. Estos límites impiden certificar consumo, aunque varias correspondencias documentales sí están demostradas.

## Implementación autorizada después del diagnóstico

Mauricio autorizó proceder. Se añaden `InsumoAuditSnapshot` (proyección versionada,
no maestro ni movimiento) e `InsumoAuditReview` (explicación/aprobación por otra
persona). No se aplica ninguna equivalencia nueva ni reclasificación. Las
cantidades de las fuentes históricas anteriores corresponden al corte indicado,
no a una verificación futura.

La consulta vive en `/reportes/auditoria-compras-produccion/`. Reutiliza acceso de
Reportes; registrar revisiones exige gestión de Reportes, sin ampliar permisos.
Los expedientes y sus revisiones no modifican consumidores financieros.

La tarea `inventario.auditar_compras_produccion` relee mes actual/anterior diariamente
09:00 America/Mazatlan. Difiere ante el último ciclo Point pendiente/activo por tipo o lock de periodo,
con hasta tres reintentos cada 30 minutos. Un job fallido/incompleto se publica como
límite de fuente. No se disparan sincronizadores ni notificaciones. Cambios más
antiguos requieren reauditar explícitamente el mes; no se promete detección
instantánea ni cobertura externa. El piloto físico y sus contratos siguen pendientes.

`python manage.py auditar_compras_produccion --mes AAAA-MM` exporta sin escribir;
`--guardar` conserva solo el expediente. Lectura consistente PostgreSQL; lock por
mes, fingerprint y revisiones anteriores conservadas. El formulario rechaza
explicaciones sobre una revisión reemplazada y reintentos incompatibles. La
aprobación solo acepta la última explicación, de otra persona.

CFDI por folio/ventana ±7 días es candidato, no enlace aprobado; XML vacío,
inválido o mayor a 2 MB se contabiliza sin interpretación. Recepciones, conteos,
lotes y solicitudes de merma se enumeran como candidatos sin inferir vínculos.
El saldo Point actual no es frontera del mes. Los totales son registros observados,
no inventario certificado; no se suman unidades o ubicaciones. Las coincidencias
cuantitativas admiten 0.0005 unidades base exclusivamente por representación del
kardex a tres decimales, nunca como tolerancia física. El teórico usa rendimiento
actual solo si sus unidades son compatibles; no explota preparaciones dos veces.
