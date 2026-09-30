# Diseño — retornos parciales de transferencias Point

## Objetivo

La auditoría mensual debe reproducir el efecto real de Point cuando una transferencia se recibe parcialmente. También debe mostrar al repartidor real de la ruta, no al usuario que ejecutó la sincronización.

## Regla contable

- Transferencia abierta enviada: el origen descuenta lo enviado; la cantidad permanece en tránsito hasta recepción o cancelación.
- Transferencia finalizada completa: el origen descuenta lo enviado y el destino suma lo recibido.
- Transferencia finalizada parcial: Point suma al destino lo recibido y retorna automáticamente al origen la diferencia positiva entre enviado y recibido. La salida neta del origen equivale a lo recibido.
- Transferencia cancelada: no participa en el saldo mensual porque Point reintegra la salida.
- Cantidades anómalas, como recibido mayor que enviado, no se corrigen por inferencia; permanecen como excepción auditable.

## Fuente y flujo

`PointTransferLine` sigue siendo la fuente persistida y única para la auditoría. El cálculo mensual usa sus identificadores, origen, destino, fechas, cantidades y estados. No se descarga nuevamente la transferencia ni se crea otra representación.

Para una recepción parcial finalizada:

1. conservar enviado y recibido como evidencia;
2. calcular `retorno = enviado - recibido`;
3. conservar la salida enviada en la fecha de envío;
4. aplicar el retorno como entrada al origen en la fecha de recepción;
5. aplicar al destino la entrada recibida;
6. mantener `TRANSFER_QUANTITY_MISMATCH` como explicación y mostrar el retorno Point, no como faltante en tránsito.

## Interfaz de revisiones

La columna **Repartidor** de las discrepancias usa `caso.ruta.repartidor.user`. El actor técnico permanece en `creado_por` para auditoría, pero no se presenta como conductor.

## Alcance técnico

- Ajustar el cálculo compartido de transferencias en `BranchInventoryTraceabilityService`.
- Actualizar la explicación de la discrepancia para distinguir retorno Point de una transferencia abierta.
- Corregir una celda en `logistica/revisiones_entrega.html`.
- Añadir pruebas mínimas para transferencia abierta, completa, parcial finalizada y repartidor real.
- No agregar modelos, migraciones, dependencias ni escrituras a Point.

## Validación

- El caso 37934 debe producir salida 2 y retorno 1 en CEDIS, entrada 1 en Bamoa y salida neta 1.
- Una reconstrucción de agosto debe reducir el falso faltante del origen sin modificar el cierre Point.
- Los casos abiertos continúan identificados como tránsito.
- La bandeja muestra Carlos Anaya para `RUT-202608-0029`; Mauricio permanece únicamente como creador técnico del registro.
- Segunda reconstrucción idempotente y sin notificaciones duplicadas.

## Riesgos y límites

El retorno derivado solo se acepta cuando Point marca la transferencia recibida y finalizada. Si Point expone posteriormente el movimiento 25 como fuente persistida independiente, se deberá evitar doble conteo; esta versión no crea ese segundo ingreso.
