# Auditoría de inventario — primer quiebre diario comprobado

## Estado

Diseño aprobado conceptualmente por Dirección el 30 de septiembre de 2026. Amplía la auditoría mensual y el agente auditor existentes. No autoriza modificar movimientos, cierres ni existencias de Point.

## Problema

La auditoría mensual explica el balance completo, pero una diferencia final obliga a revisar todo el periodo. Para 3 Pecados Chico en CEDIS se sabe que debían quedar 16 unidades y Point cerró con 6; todavía falta reducir la investigación al intervalo donde aparecieron las 10 unidades no localizadas.

La base conserva movimientos con fecha y snapshots periódicos de existencia. Deben reutilizarse para responder:

1. cuál fue el último corte observado que todavía cuadraba;
2. cuál fue el primer corte observado que dejó de cuadrar;
3. qué ventas, producción, mermas, transferencias, retornos y conversiones ocurrieron entre ambos;
4. qué parte queda comprobada y qué cantidad continúa sin localizar.

## Enfoques considerados

### 1. Reconstrucción al abrir cada expediente

Reduce cambios iniciales, pero repite consultas y deja al agente sin una visión global del mes.

### 2. Proyección mensual reutilizando snapshots y movimientos

Es el enfoque elegido. Carga una vez las fuentes del mes, calcula los puntos de control de todos los expedientes y guarda un resumen regenerable en la investigación existente.

### 3. Nuevo libro normalizado de movimientos

Podría ofrecer otra representación completa, pero duplicaría fuentes Point ya persistidas, exigiría migraciones y abriría riesgo de divergencia. Se descarta.

## Regla de auditoría

Para cada `mes + sucursal canónica + producto`, el servicio parte del cierre protegido del mes anterior y aplica, en fecha operativa local:

```text
saldo anterior
+ producción
+ transferencias recibidas y retornos Point
+ conversiones de entrada confirmadas
- ajustes identificados
- ventas
- mermas
- transferencias enviadas
- conversiones de salida confirmadas
= saldo reconstruido
```

En cada fecha con snapshot persistido se toma la última observación disponible de la fecha local y se compara con el saldo reconstruido hasta ese corte.

- Si coincide, queda como último corte correcto.
- Si no coincide, queda como corte con diferencia observada.
- El primer corte diferente posterior al último correcto define el primer intervalo de quiebre.
- Si nunca hubo un corte correcto intermedio, el intervalo comienza en la apertura mensual.
- Si faltan snapshots, no se inventa una fecha: permanece “sin corte intermedio comprobable”.

La comparación utiliza `America/Mazatlan`. Una observación no se presenta como cierre oficial del día.

### Movimientos sin hora y rango conservador

Las ventas y parte de la producción solo informan fecha. Para un snapshot tomado durante esa misma fecha no es posible saber cuáles de esos movimientos ya habían ocurrido. El servicio no escogerá un orden arbitrario:

1. aplica exactamente los movimientos con fecha-hora anterior al snapshot;
2. calcula, con los movimientos sin hora de la fecha, el saldo mínimo posible si primero ocurrieron todas las salidas;
3. calcula el saldo máximo posible si primero ocurrieron todas las entradas;
4. considera el corte compatible cuando el stock observado cae dentro de ese rango;
5. declara diferencia comprobada solo cuando el stock observado queda fuera del rango completo.

Un corte compatible no equivale a “cuadrado”; queda como `INCONCLUSIVE` hasta que un corte posterior o el cierre mensual permita comprobarlo. Así se evita señalar un falso quiebre por desconocer el orden intradía.

## Precisión y límites

`PointDailySale` conserva ventas por día, no la hora de cada ticket. Por eso el resultado identifica un intervalo diario comprobado, no necesariamente una transacción culpable. Los movimientos con hora posterior al snapshot no pueden imputarse a ese corte. El servicio debe separar:

- movimientos aplicables antes del corte;
- movimientos diarios sin orden intradía suficiente;
- evidencia ausente o ambigua.

La interfaz usa “primer corte con diferencia” y “movimientos a revisar”; nunca “movimiento causante” sin evidencia exacta.

## Fuentes y reutilización

Se reutilizan exclusivamente:

- cierres históricos Point protegidos;
- `PointInventorySnapshot`;
- ventas diarias;
- producción;
- mermas;
- transferencias, recepciones y retornos;
- conversiones conservadoras;
- alias canónicos de sucursales;
- expediente e investigación del agente auditor.

No se consulta Point en tiempo real, no se reingresan datos y no se crea una tabla de movimientos. La ficha de fuentes está en `docs/data-reuse/2026-09-30-auditoria-inventario-quiebre-diario.md`.

## Arquitectura mínima

### Proyector de puntos de control

Un servicio en `pos_bridge/services` carga los movimientos y snapshots del mes en consultas acotadas, aplica las reglas compartidas de identidad y produce por clave:

- último corte correcto;
- primer corte diferente;
- saldo reconstruido y stock observado;
- diferencia del corte;
- IDs de movimientos dentro de la ventana;
- advertencias de precisión o cobertura.

No persiste datos por sí mismo.

### Integración con el agente auditor

La corrida mensual del agente calcula la proyección una sola vez y añade al `investigation_summary` existente:

```json
{
  "daily_break": {
    "status": "FOUND | NOT_FOUND | INSUFFICIENT_EVIDENCE",
    "last_matching_checkpoint": null,
    "first_mismatch_checkpoint": null,
    "reconstructed_stock": "0",
    "observed_stock": "0",
    "difference": "0",
    "movement_ids_by_source": {},
    "warnings": []
  }
}
```

El estado interno también admite `INCONCLUSIVE` para meses con snapshots compatibles pero sin un punto intermedio exacto. La pantalla lo traduce a “corte compatible; orden del día no comprobable”.

La huella de investigación incorpora esta proyección. Una segunda ejecución idéntica no modifica expedientes ni duplica notificaciones.

### Detalle del expediente

Debajo de la secuencia mensual se muestra un bloque compacto:

- último corte que cuadró;
- primer corte con diferencia;
- diferencia observada;
- movimientos del intervalo agrupados por tipo;
- unidades aún no localizadas;
- advertencia cuando la precisión es diaria o la cobertura es insuficiente.

Cada grupo reutiliza los enlaces de evidencia existentes. No se añade otra tabla masiva ni información técnica de modelos.

## Clasificación del resultado

Las unidades del intervalo pueden quedar como:

- vendidas;
- mermadas;
- convertidas con origen confirmado;
- transferidas o retornadas;
- trasladadas a Devoluciones;
- ajustadas con referencia identificada;
- no localizadas.

Las categorías se derivan solo de movimientos existentes. El remanente se calcula, no se reclasifica automáticamente como merma.

## Rendimiento

- Una corrida trabaja un solo mes.
- Cada fuente se consulta en lote, no una vez por expediente.
- Los snapshots se reducen al último corte por fecha local, ubicación y producto.
- La pantalla lee la investigación materializada y no reconstruye agosto en cada apertura.
- No se recorren otros meses salvo la apertura protegida ya seleccionada por el balance mensual.

## Manejo de errores

- Falta de apertura o cierre protegido: conserva `fuente incompleta`.
- Sin snapshots intermedios: `INSUFFICIENT_EVIDENCE`.
- Producto o ubicación ambiguos: no se fusionan por nombre.
- Conversión sin origen: la entrada se conserva; la salida no se inventa.
- Transferencia parcial finalizada: reutiliza el retorno Point ya corregido.
- Stock observado negativo: se muestra como evidencia, no se corrige.
- Un fallo de la proyección diaria no borra la investigación mensual previa.

## Pruebas

Pruebas unitarias mínimas:

1. encuentra el primer corte diferente después de uno correcto;
2. inicia la ventana en la apertura cuando el primer snapshot ya difiere;
3. respeta `America/Mazatlan` en límites de fecha;
4. no aplica un movimiento posterior al corte;
5. usa un rango conservador para ventas o producción sin hora y no genera un falso quiebre;
6. agrupa transferencias y retornos sin doble conteo;
7. conserva conversión sin origen como advertencia;
8. no ejecuta consultas por expediente;
9. segunda investigación idéntica es idempotente;
10. el detalle muestra la ventana sin exponer nombres técnicos.

## Criterios de aceptación

- Todos los expedientes auditables de agosto reciben un estado de quiebre diario sin crear expedientes nuevos.
- 3 Pecados Chico/CEDIS muestra el primer intervalo comprobable y los movimientos de esa ventana.
- El remanente de 10 unidades permanece no localizado mientras ninguna fuente lo explique.
- Los casos sin snapshots dicen que la evidencia es insuficiente; no presentan una fecha falsa.
- No hay descarga nueva, tabla nueva, migración ni duplicación de Point.
- La ejecución mensual carga las fuentes en lote y es idempotente.
- El resultado se valida en la pantalla autenticada de producción después del despliegue.

## Fuera de alcance

- Descargar o persistir ahora `/Stock/GetHistorial` para cada producto.
- Modificar o compensar movimientos Point.
- Declarar mermas automáticamente.
- Capturar conteos físicos nuevos.
- Identificar una transacción exacta cuando la fuente disponible solo ofrece fecha diaria.
