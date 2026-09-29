# Agente auditor de inventario — Etapa 1

## Estado y alcance aprobado

Dirección aprobó el 29 de septiembre de 2026 extender la auditoría mensual existente para que actúe como un encargado: registre todas las diferencias, reconstruya su trazabilidad con las fuentes disponibles, asigne y notifique inmediatamente solo los casos relevantes, agrupe los casos menores y conserve toda decisión sensible para aprobación humana.

Esta etapa cubre inventario de agosto de 2026 y Logística. Reutiliza el balance mensual, los expedientes, eventos, permisos y notificaciones existentes. No crea otra bandeja, otro catálogo ni otra descarga de Point.

## Resultado observable

Al abrir la auditoría mensual, las excepciones aparecen ordenadas por atención. Cada expediente informa:

- qué hechos comprobó el agente;
- qué relación encontró con Point o Logística;
- qué hipótesis sigue siendo candidata y qué dato falta;
- qué área debe responder y, cuando existe una identidad canónica autorizada, a qué persona se asignó;
- por qué el caso es prioritario o quedó agrupado para revisión.

Los casos relevantes producen una sola notificación idempotente por versión investigada. Una reconstrucción idéntica no duplica expedientes, asignaciones ni avisos.

## Fuentes reutilizadas

El agente lee, sin reimportar:

- `ProductInventoryAuditCase`, `ProductInventoryAuditRun` y `ProductInventoryAuditEvent` como expediente mensual;
- `PointHistoricalInventoryClosingLine`, `PointDailySale`, `PointProductionLine`, `PointWasteLine`, `PointConversionLine` y `PointTransferLine` mediante `source_trace`;
- `RutaCargaChecklistLinea` y `DiscrepanciaLogistica` para relacionar una transferencia Point con su carga, recepción y aclaración logística;
- RRHH (`Empleado`) y acceso operativo (`UserModuleAccess`) para localizar jefaturas por área sin asignar usuarios por similitud de nombre;
- `Notificacion` y `core.notificaciones.crear_notificacion` para la bandeja ya existente.

## Clasificación conservadora

Todos los casos quedan registrados. La prioridad no declara culpabilidad ni causa.

### Atención alta

Un caso es relevante y se atiende inmediatamente cuando ocurre cualquiera de estas condiciones comprobables:

- existe una discrepancia logística abierta ligada a una transferencia exacta del expediente;
- falta el origen o destino de una conversión;
- existe una transferencia con cantidad incompatible;
- el mismo producto y ubicación repite una excepción en otro mes materializado;
- el saldo esperado es negativo.

### Revisión normal

Las demás diferencias no resueltas se conservan para revisión, ordenadas por cantidad absoluta. Una diferencia de hasta una unidad, sin riesgo estructural ni repetición, queda agrupada como menor.

### Fuente incompleta

Una fuente incompleta conserva su estado y se resume por corrida. No genera cientos de notificaciones por producto. Tampoco se convierte en cero ni en una supuesta merma.

No se usa un umbral monetario en esta etapa porque `PointProduct.precio` es precio actual y no prueba el valor histórico de agosto. Incorporarlo sin una fuente histórica confiable produciría una prioridad engañosa.

## Asignación

El agente asigna un área en todos los casos investigados y una persona únicamente cuando la identidad puede resolverse de forma canónica:

- discrepancia de ruta o transferencia: responsable ya asignado en `DiscrepanciaLogistica`; en su ausencia, responsable con gestión de Logística;
- producción, merma o conversión en CEDIS: jefatura activa de Producción en RRHH;
- venta o diferencia de sucursal: jefatura activa de Ventas en RRHH;
- fuente incompleta o causa no identificada: jefatura activa de Administración en RRHH.

Si hay más de una candidatura válida o no existe ninguna, el área queda asignada y la persona permanece `Sin asignar`. El agente no escoge por nombre, antigüedad ni primera coincidencia.

## Investigación estructurada

La investigación persiste como proyección regenerable dentro del expediente:

- `facts`: hechos respaldados por IDs de fuente;
- `hypotheses`: explicaciones candidatas claramente marcadas;
- `missing`: evidencia necesaria para concluir;
- `related_logistics_discrepancy_ids`: discrepancias exactas relacionadas;
- `recurrence_count`: otros meses con excepción para el mismo producto y ubicación;
- `grouping_key`: ubicación, área y causa para agrupar menores.

La investigación tiene una huella calculada con el balance, incidencias y relaciones encontradas. La huella permite repetir el proceso sin duplicar avisos.

## Notificaciones y seguridad

- Solo prioridad alta con persona resuelta genera notificación inmediata.
- La notificación enlaza al expediente existente.
- La misma huella no vuelve a notificar.
- Una huella nueva puede notificar otra vez si el caso sigue siendo relevante.
- Notificar o asignar no concede permisos nuevos.
- Explicar, aprobar o rechazar conserva las reglas actuales de custodia y separación entre registrante y aprobador.
- El agente nunca modifica Point, inventario, merma, conversión, venta, transferencia o cierre.

## Interfaz

La lista mensual conserva seis columnas alineadas y el encabezado fijo. No añade una tabla técnica. La columna Estado muestra, en orden:

- nivel de atención;
- estado de conciliación;
- área o persona responsable.

Los filtros agregan únicamente `Atención`. El detalle incorpora un bloque `Investigación del agente` con hechos, hipótesis y pendientes en lenguaje operativo. Los nombres de tablas y jobs permanecen ocultos.

## Ejecución y cierre de agosto

La implementación añade un comando idempotente con `--dry-run` y `--month`. En producción se ejecutará primero en seco. Como la corrida de agosto está actualmente en `SOURCE_INCOMPLETE`, esta tarea no reconstruye ni protege nuevamente el cierre Point; investiga los expedientes ya materializados y conserva su estado.

## Criterios de aceptación

- Los 2,026 expedientes actuales de agosto permanecen en la misma tabla y no se duplican.
- Los 506 casos con fuente incompleta no crean 506 notificaciones.
- La discrepancia logística abierta de 3 Pecados Chico se relaciona por la transferencia Point exacta y conserva a su responsable actual.
- Una ejecución repetida con las mismas fuentes no duplica notificaciones.
- Una relación ambigua no asigna una persona por intuición.
- Los casos menores siguen visibles y agrupables, pero no interrumpen al responsable.
- La pantalla mantiene columnas alineadas, encabezado fijo y términos operativos.
- Ninguna acción del agente cambia movimientos, cierres o cantidades de Point.

