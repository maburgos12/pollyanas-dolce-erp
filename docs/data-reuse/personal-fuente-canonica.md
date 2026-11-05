# Ficha de fuentes — puestos canónicos y comprobantes de gastos

Fecha: 2026-11-04. Registros contrastados en producción; pruebas en PostgreSQL 16 aislado.

## Necesidad y unidad de análisis

Un puesto por expediente, conservando la función operativa, ubicación y estatus como conceptos distintos. Un comprobante fiscal original vinculado al gasto cuyo importe respalda; sin copiar facturas ni inferir su pago.

## Fuentes candidatas

| Concepto | Modelo / tabla | Quién actualiza | Identificador / ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Persona y puesto | rrhh.Empleado | Capital Humano | PK, código, función operativa | Expedientes 17, 29, 5 y otros activos tienen EMBETUNADO aunque su texto de puesto esté vacío | Empleados, organización, asignación, reportes y API |
| Función canónica | rrhh.CatalogoFuncionOperativa | Catálogo de Capital Humano | código estable y etiqueta | EMBETUNADO y ARMADO tienen entradas diferentes | Helper compartido y guardado de Empleado |
| Baja real | rrhh.EmpleadoBaja | Capital Humano | empleado y fecha | Personas 48 y 98 tienen bajas documentadas; campo puesto vacío no demuestra baja | Expediente y conciliación del costo histórico |
| Gasto | reportes.GastoOperativoMensual / ObligacionGasto | Captura/importación financiera | PK y cobertura mensual | Gastos 2122–2131 tienen importe y cobertura, pero sin enlace fiscal; 2131 es licencia anual de $3,004.40 | Lector de gastos, planeación y exportación |
| Comprobante original | sat_client.CfdiDescargado | Descarga SAT | UUID, RFC, total, estado, XML | CFDI 8884 coincide en proveedor, concepto e importe de licencia; CFDI 7693 comparte importe pero es uniformes y se descarta | Validación del vínculo y consulta de gastos |

## Alias y equivalencias

| Términos | Estado | Evidencia / caso contrario | Revisión |
| --- | --- | --- | --- |
| EMBETUNADO / Embetunado | Confirmada | Misma clave del catálogo existente | Normalizar solo etiqueta |
| Cajera / Cajero / Cajas | Confirmada por Mauricio | Solo con función CAJAS; auxiliar y encargado se conservan | Respetar puestos específicos |
| Armado / Embetunado | Distinta | Códigos independientes y corrección explícita del usuario | No fusionar |
| Puesto vacío / baja | Distinta | Hay empleados activos con puesto operativo registrado | Nunca inferir estatus |
| Mismo importe / misma factura | No implica equivalencia | Facturación y uniformes comparten $3,004.40 | Contrastar concepto, proveedor, cobertura y XML antes de vincular |

## Decisión

Reutilizar el catálogo y la tabla Empleado, normalizando el texto únicamente cuando es vacío o un alias de su propia función. Conservar puestos específicos. Regularización revisable por SHA256, transaccional y con AuditLog; no modifica salarios, bonos, funciones ni estatus.

Extender GastoOperativoMensual con una FK nullable y protegida al CFDI existente. El servicio exige permiso de gestión, motivo de conciliación, importe y XML congruentes, comprobante recibido vigente en MXN y exclusividad del vínculo. No demuestra pago ni vigencia de póliza por sí solo. El lector vuelve a validar el CFDI y comprueba la disponibilidad real de archivos, en vez de aceptar una cadena de texto como prueba. El global excluye importes parciales de su subtotal respaldado.

Procedimiento: `inventario_fuentes_datos --term puesto --term empleado --term soporte --term gasto --skip-checks` con PostgreSQL aislado; check de Django ejecutado por separado. Consultas de producción acotadas por IDs anteriores. Evidencia privada y hashes en el directorio de trabajo externo `personal-fuente-canonica`; no se versionan nómina, RFC ni XML.

Límites: una FK no subsana ausencia de soporte ni acredita el pago. Las diferencias históricas entre nómina fiscal y tabular requieren conciliación aparte; esta modificación no cambia importes de nómina ni certifica un cierre integral.
