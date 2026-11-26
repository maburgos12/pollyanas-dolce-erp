# Evidencia compartida de inventario Point

Cada observacion conserva su fila de inventario, sucursal, producto,
cantidades, hora y trabajo de sincronizacion. No se cambia la identidad
ni el orden de los cortes. El cierre, la apertura y el conteo siguen
utilizando cada observacion con su trabajo original, incluso cuando los
datos que Point devolvio no variaron.

Para las capturas nuevas, el importador puede guardar el documento JSON
exacto una sola vez y referenciarlo en las observaciones sucesivas.
Compara tambien el contenido antes de reutilizar una huella. No mueve
ni borra observaciones antiguas, archivos crudos o respaldos. Esta
medida reduce la duplicacion del documento; no elimina observaciones
ni cambia la rotacion de respaldos.

## Activacion controlada

1. Publicar codigo y migracion con `POINT_INVENTORY_SHARE_PAYLOADS`
   desactivado. La migracion agrega una referencia opcional sin
   reescribir el historico; la restriccion verifica las escrituras nuevas.
2. Verificar que todos los procesos del ERP que leen cortes ya tienen
   la revision nueva antes de habilitar el escritor.
3. En dos capturas iguales, verificar dos observaciones con fecha y
   trabajo propios, un documento compartido, y valores de apertura,
   cierre y unidad iguales a los consultados antes del cambio.
4. Para detener nuevas escrituras compartidas, desactivar la opcion.
   Conservar los lectores compatibles: bajar a una revision anterior
   despues de activar requeriria materializar la evidencia referenciada
   antes de quitar compatibilidad. No revertir la migracion ni borrar tablas.

Para medir el ahorro, comparar muestras de volumen y repeticion de los
documentos y el crecimiento real de tablas e indices; no contar
observaciones conservadas como espacio liberado.

## Historico de evidencia

Despues de validar los lectores, activar el escritor y comprobar un respaldo
restaurable del historico, ejecutar por dia cerrado de Mazatlan:

```text
manage.py compact_point_inventory_evidence YYYY-MM-DD --batch-size 500
manage.py compact_point_inventory_evidence YYYY-MM-DD --batch-size 500 --apply
manage.py compact_point_inventory_evidence YYYY-MM-DD --batch-size 500 --after-id ID --apply
```

La primera llamada no escribe. Cada llamada aplica un solo lote atomico y
devuelve `next_id` para el siguiente. Un dia se da por terminado al recibir
`selected=0` y volver a comprobar desde el inicio. Si hay bloqueo, diferencia
de contenido o error, no avanzar el cursor. Se compara el JSON original sin
redondear numeros ni devolver la evidencia a Python para serializarla.

La compactacion comparte documentos, no fusiona saldos ni cortes. PostgreSQL
puede reutilizar paginas al aspirar las versiones antiguas; eso no equivale a
liberar el mismo espacio en el VPS. No ejecutar `VACUUM FULL` ni una reescritura
de la tabla como parte de estos lotes.

## Saldos por dia sin duplicar cortes

La clave de un saldo es el dia operativo de Mazatlan, los IDs Point de sucursal
y producto, y las cantidades exactas de saldo, minimo y maximo. Los cortes
siguen teniendo sus IDs, horas y trabajos originales. La vista compatible
resuelve sus cantidades desde el estado diario o desde las columnas historicas
si aun no se compactaron. Las escrituras nuevas crean o reutilizan el estado
desde PostgreSQL; un cambio de sucursal, producto o dia no comparte saldo.

La migracion conserva los indices y la secuencia de corte, sin copiar el
historico ni reescribir la tabla. Aplicarla solo despues de verificar una
restauracion NAS y comprobar el flujo de captura y lectura. Validar el plan y
la latencia de los consumidores sobre copias representativas antes del cambio.
No volver a ejecutar migraciones antiguas que traten la vista como tabla.

Tras el despliegue, revisar un lote de dia cerrado y aplicar el mismo comando
solo con el respaldo validado:

```text
manage.py compact_point_inventory_balances YYYY-MM-DD --batch-size 500
manage.py compact_point_inventory_balances YYYY-MM-DD --batch-size 500 --max-batches 50 --apply
manage.py compact_point_inventory_balances YYYY-MM-DD --batch-size 500 --max-batches 50 --after-id ID --apply
```

Cada lote confirma por separado antes de imprimir su cursor. Repetir hasta
`selected=0` y confirmar desde el inicio del dia. La comparacion
de cada lote conserva las cantidades, IDs, horas, trabajos y referencias de
evidencia; un fallo revierte el lote entero. Medir espacio recuperable aparte;
esta operacion no usa `VACUUM FULL`, no retira respaldos y no reinicia el VPS.
