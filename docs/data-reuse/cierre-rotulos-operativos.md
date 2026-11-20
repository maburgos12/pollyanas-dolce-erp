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
- Los 22 bucles de `module_tabs` de Reportes reutilizan un único parcial
  `reportes/_module_tabs.html`; sus etiquetas, URLs y selección activa no cambian.
  La causa compartida es el grid limitado más `white-space: nowrap`, no el cierre.
  Una regla de `hallmark_guardrails.css`, limitada al marcador exclusivo
  `.reportes-module-tabs` desde 761px, envuelve los nombres largos. El cargador
  existente del CSS cambia de versión; el scroll móvil queda intacto.
- La familia responsive `report-tabs`, que ya usaban ocho llamadas, se declara
  en el parcial junto al marcador exclusivo (las demás usaban `module-tabs` solo).
  Se conservan las variantes: dos aria-label Reportes y seis Secciones de reportes;
  las dos cédulas
  IMSS conservan su etiqueta `section` y la condición exterior `if module_tabs`.
  Planeación conserva literalmente su regla de impresión; la familia explícita
  de su llamada sigue siendo `report-tabs`, también reconocida por Hallmark.
  La separación de espacios del atributo class de Auditoría de insumos se
  normaliza sin alterar ninguna clase ni selección. Los menús estáticos de BI,
  Ventas y otros reportes que no usan ese bucle quedan fuera de la extracción.
  El marcador no se incorpora a CRM, Compras, Logística ni Mantenimiento.
- Las tres referencias del SW se alinean en versión 20; el fetch/scope no cambia.
  La URL del CSS propio cambia para entregar el ajuste.

## Verificación y límites

Las pruebas de render comprueban texto operativo, procedencia técnica cerrada,
importe/fecha/contexto intactos, dominio futuro y aviso de plan ausente. La navegación
conserva orden, URLs, active, clases, aria y envoltura; Cierre y Presupuesto Maestro
renderizan el mismo componente. Se compilan los 22 consumidores y se ejecutan
los checks afectados en PostgreSQL16 propio.
La geometría local es evidencia de preparación; la aceptación real en móvil y
escritorio queda pendiente del turno de publicación posterior a #1719 y mensual.
Sin consultas productivas ni cambios de datos.
