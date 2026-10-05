# Ficha de fuentes — representación original de fronteras

Fecha/ambiente: 4–5 octubre 2026, diagnóstico VPS read-only y prueba PostgreSQL16 local aislada. Autorización: adjunto humano de reparación de lectores Bamoa, conservando originales/controles.

## Necesidad y unidad de análisis

Una ecuación original y su instante por FK_Movimiento dentro de la canónica exacta. Evitar dos falsos vetos de representación, sin transformar cobertura, inventar membresía o acreditar físico.

## Fuentes candidatas

|Concepto|Modelo / tabla|Creador|Identidad y ámbito|Evidencia|Consumidores|
|---|---|---|---|---|---|
|Historia original|PointProductHistoryRow / pos_bridge_product_history_rows|AuditStockHistoryService|import_record + FK_Movimiento; producto/sucursal exactos|Import785 row138671 FK505190: raw2022-10-30T01:18:31.093 y persistido07:18:31.093Z mismo instante fold local; import816 diez VENTA con qty1.0 y residuos4e-17|lector fronteras, balance, materializador, cierre|
|Canónica|PointProductHistoryImport|capture/ingreso oficial|PK785/816 branch5, product285/532|COMPLETE500 retenidas, membresía ausente conservada|reconcile/fronteras|
|Frontera independiente|PointInventorySnapshot|job inventory SUCCESS|par/dominio/manifiesto/corte exactos|8 fronteras aceptadas/4 vetadas en aceptación1471, raws intactos|documentary_historical_boundary|

Inventario local ejecutado con términos historial/snapshot; sólo candidatos léxicos, no identidad. Migraciones completas/check0 antes de implementar. La consulta VPS original de contradicciones está en diagnostico-contradiccion-bamoa-816.py; aceptación íntegra1471 y SHA archivados en el expediente del hilo. No repetir HTTP.

## Alias y equivalencias

UTC y zona aware son equivalentes sólo para el mismo instante exacto: comparar ambos convertidos UTC. Una diferencia1µs sigue distinta. No cambia interpretación naïve por dominio, parser ni America/Mazatlan.

Los tres operandos originales float finitos representan números binarios. La comparación Decimal exacta conserva precedencia. Sólo si todos son float se certifica la ecuación con centros Fraction.from_float y radio sumado de medias ULP. Strings/int/Decimal/mixed/bool no reciben esta rama. Ninguna identidad/cancelación/fecha ni stock snapshot recibe tolerancia.

## Decisión de diseño

Extender exclusivamente el veto documental compartido, sin otra fuente/importador/modelo. No redondear una ecuación para cuadrar: el radio IEEE exacto determina admisión y se rechaza certificación si alcanza quantum decimal más fino del modelo dividido por10. Esa barrera conservadora por magnitud NO es tolerancia de aceptación. Pruebas explícitas deniegan0.0004, cantidad2/delta1, strings con residuo, no finitos, diferencias persistidas y stocks enormes. Raw/membership/coverage/fingerprints conservados.

## Entorno propio y entrega

Tarea auditor-representacion-original ownercodex; rama codex/auditor-representacion-original; base b3317f74. Compose erp_auditor_representacion_original, PostgreSQL5477/Redis6477, red10.239.77.0/24, volúmenesdb_data/redis_data exclusivos. Contenedores11f824159d4a yb8f2055805c6. No producción operativa en esta DB; respaldar/verificar mediante helper al cierre oficial. Venv raíz sólo runtime compartido, no borrar.

RED confirmado para ambos falsos vetos y para signo/cantidad cero antes de corregir. GREEN: 57 pruebas específicas y 453 regresiones compartidas PASS (48.642s), check0, migrate--check0, makemigrations--check--dry-run sin cambios y diff--check0. Revisión independiente sin bloqueantes. El testDB preservado advierte reglas críticas ausentes tras flush; el check de la DB local principal no tiene incidencias. No se modificaron reglas para ocultarlo.

Pendiente: CI SHAactual, merge/deploy oficial y aceptación read-only doble de los dos pares/autenticada. La publicación no es cierre de septiembre.
