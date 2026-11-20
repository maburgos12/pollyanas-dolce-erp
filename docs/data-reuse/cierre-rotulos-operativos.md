# Cierre: rótulos operativos y navegación legible

## Necesidad y alcance

Dirección necesita entender el origen de la lectura sin conocer nombres de modelos.
La pantalla conserva las mismas cantidades, fechas, fuentes, estados y permisos.
La consulta diaria, los importadores y el contexto de la vista no cambian.

## Fuentes existentes y reutilización

- `daily_operational_closure_service.build_daily_operational_closure` conserva
  `domains`, `source_truth`, `visible_source`, `plan_note` y `source_notes`.
- `cierre_operativo_diario.html` muestra los cuatro dominios conocidos con rótulos
  operativos. Los valores técnicos originales siguen disponibles en `details`
  cerrados y accesibles por teclado. Un dominio desconocido conserva su referencia
  sin inventar procedencia.
- El plan sigue separado de la producción Point; las mermas de sucursales y los
  insumos siguen separados. El stock actual de una ubicación no se presenta como
  cierre histórico global. Los 75 registros por acreditar no se dan por resueltos.
- No se crean capturas, tablas, equivalencias ni contratos de datos. No es necesario
  ejecutar inventario de fuentes: este cambio solo presenta campos ya consumidos.

## Consumidores y presentación

- La vista `reportes:cierre_operativo` y su template son el consumidor afectado.
- El menú reutiliza `module_tabs` de Reportes, las mismas URLs y componentes.
  La geometría local confirmó solapes de texto en escritorio por el grid de ancho
  limitado más `white-space: nowrap`. Solo el CSS de `.closure-shell` permite
  envolver las etiquetas desde 761px; el scroll horizontal móvil permanece igual.
  Ningún otro menú de Reportes o módulo recibe esta regla.
- Las tres referencias del SW se alinean en versión 20; el fetch/scope no cambia.
  La URL del CSS propio cambia para entregar el ajuste.

## Verificación y límites

Tres pruebas de render comprueban texto operativo, procedencia original disponible
pero cerrada, cantidades/fecha y conservación del contexto, incluido dominio futuro y aviso de plan ausente.
Se ejecutan los consumidores de Cierre y los gates existentes en PostgreSQL16 propio.
La geometría local es evidencia de preparación; la aceptación real en móvil y
escritorio queda pendiente del turno de publicación posterior a #1719 y mensual.
Sin consultas productivas ni cambios de datos.
