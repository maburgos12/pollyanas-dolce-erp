# Auditoría de inventario — resolución por historial Point

## Estado y alcance aprobado

Diseño aprobado por Dirección el 30 de septiembre de 2026. Amplía la auditoría mensual y el agente auditor existentes para consultar el historial transaccional de Point únicamente cuando el balance normal de un producto, sucursal y mes conserva una diferencia. No modifica inventarios, movimientos, cierres ni mermas en Point.

## Problema comprobado

La auditoría de agosto dejó el caso 79, `Pastel de 3 Pecados Chico` en CEDIS, con apertura 23, producción 518, entradas 4, salidas 529, cierre esperado 16 y cierre Point 6. Las fuentes agregadas no incluyeron las salidas por conversión del producto origen.

El historial vivo de Point acredita ocho movimientos de salida por conversión por 10 pasteles y dos entradas por conversión. Su balance completo es:

```text
23 apertura
+ 518 producción
+   2 entradas por conversión
+   4 ajustes de entrada
+   2 retornos de transferencia
-  10 salidas por conversión
- 533 salidas por transferencia
=   6 cierre Point
```

Por tanto, las 10 piezas no son un faltante físico: Point registra su salida por conversión. El efecto neto de conversiones sobre el producto fue de menos 8 piezas. La coincidencia temporal con entradas de 64 rebanadas es evidencia contextual, no una relación origen-destino explícita, por lo que el sistema no la declarará como identidad confirmada.

## Alternativas consideradas

### Consultar Point cada vez que se abre la pantalla

Se descarta porque haría lenta la interfaz, repetiría solicitudes y volvería frágil la auditoría ante una sesión expirada.

### Crear otra tabla de movimientos

Se descarta porque duplicaría el modelo de historial ya existente y exigiría una migración sin aportar una identidad nueva.

### Resolución por excepción con caché existente

Es el enfoque elegido. El agente usa una sola sesión Point, consulta únicamente casos con diferencia, persiste los movimientos de forma idempotente en `PointProductHistoryImport` y `PointProductHistoryRow`, y reutiliza ese historial en corridas posteriores.

## Diseño funcional

1. La auditoría mensual continúa usando cierres, ventas, producción, mermas, transferencias, conversiones y ajustes ya persistidos.
2. El agente selecciona solo expedientes con diferencia y sin historial suficiente para el mes.
3. Una captura transaccional usa el cliente existente `PointHttpSessionClient.get_stock_history` y una sola autenticación para toda la corrida.
4. Cada par sucursal-producto conserva una importación canónica identificada por una huella determinista. `FK_Movimiento` de Point se usa como `row_number`; una repetición actualiza el mismo movimiento y no crea duplicados.
5. Las corridas posteriores leen primero el historial persistido. Solo vuelven a consultar cuando la cobertura no alcanza el mes solicitado.
6. Los movimientos cancelados se conservan como evidencia, pero no afectan el cálculo.
7. El reconciliador suma por fecha operativa `America/Mazatlan` y clasifica producción, venta, merma, transferencia, retorno, conversión y ajuste a partir del tipo informado por Point.
8. La evidencia corrige exclusivamente el cálculo derivado del expediente. Nunca crea una merma, ajuste, transferencia ni conversión en Point.

## Cobertura y autoridad

El historial es suficiente solo cuando permite acreditar la apertura y el cierre del mes: debe contener un movimiento límite anterior o dentro del inicio y otro al cierre, o una frontera posterior no truncada conforme a las reglas existentes de cierre histórico. Si el límite máximo de Point no alcanza el periodo, el agente marca `historial insuficiente` y conserva la diferencia original.

Cuando la cobertura es completa, el libro transaccional de Point es la autoridad para explicar el cambio de existencia. Las fuentes agregadas continúan visibles para contrastar:

- producción reportada frente a `ENTRADA POR PRODUCCION`;
- transferencias y retornos frente al historial y Logística;
- conversiones de entrada y salida;
- mermas;
- ventas;
- ajustes de inventario.

Una discrepancia entre el reporte agregado y el historial queda registrada como diferencia de fuente; no se convierte automáticamente en pérdida física.

## Resultado del agente

`investigation_summary` incorporará un bloque compacto `point_history` con:

- estado de cobertura;
- apertura y cierre acreditados;
- totales por categoría;
- IDs de movimientos Point utilizados;
- fuentes agregadas que no coincidieron;
- saldo conciliado y remanente no explicado;
- hechos y pendientes redactados en lenguaje operativo.

Para 3 Pecados Chico el hecho principal será: “Point acredita 10 piezas de salida por conversión y 2 piezas de entrada por conversión; el efecto neto es una salida de 8 y el cierre de 6 piezas queda conciliado”. Las 64 rebanadas coincidentes se mostrarán como destino probable por fecha y cantidad, no como asignación confirmada.

## Integración mínima

- Un servicio nuevo y pequeño en `pos_bridge/services` captura o reutiliza el historial de un caso y produce una conciliación mensual.
- `InventoryAuditAgent` prepara estas conciliaciones una sola vez antes de recorrer los expedientes y las incorpora a cada investigación.
- El comando `investigate_inventory_audit_cases` activa la consulta por excepción; la reconstrucción ordinaria y la pantalla no realizan solicitudes de red.
- La vista de detalle reutiliza `investigation_summary`; no necesita una tabla masiva adicional.

## Idempotencia, rendimiento y errores

- Una sesión Point por corrida y como máximo una consulta por par sucursal-producto sin cobertura.
- Inserción/actualización por identidad Point; cero filas duplicadas al repetir.
- Las consultas de pantalla permanecen locales.
- Un error de autenticación o de un producto no aborta los demás casos: queda como fuente no disponible.
- La huella de investigación incluye la evidencia utilizada; una segunda corrida idéntica no genera otra notificación.
- No se investiga otro mes salvo el solicitado.

## Pruebas y aceptación

1. La captura repetida conserva una sola importación y una sola fila por `FK_Movimiento`.
2. Una cobertura ya suficiente evita una segunda llamada a Point.
3. Una historia truncada no se declara conciliada.
4. Los movimientos cancelados no afectan totales.
5. La zona `America/Mazatlan` decide la pertenencia al mes.
6. El agente explica conversiones de salida ausentes en el reporte agregado.
7. El balance comprobado de 3 Pecados Chico termina en 6 y remanente cero.
8. La coincidencia con rebanadas no se eleva a relación confirmada.
9. Una segunda investigación idéntica no duplica filas ni notificaciones.
10. La pantalla autenticada de producción muestra el expediente conciliado después del despliegue.

## Fuera de alcance

- Cambiar movimientos o existencias en Point.
- Declarar mermas, conversiones o ajustes automáticamente.
- Descargar el historial de productos cuyo balance ya cierra.
- Inventar relaciones origen-destino sin un identificador explícito de Point.
