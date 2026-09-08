# Resumen de compras departamentales

Propuesta aprobada por Mauricio en la conversación: resumen monetario, filtros por periodo/departamento/estado, desglose por departamento con acceso al detalle y exportación Excel equivalente.

Solo consulta: conservar autorización de la bandeja. Excluir solicitudes borrador, canceladas y completadas; excluir artículos recibidos conforme, rechazados y cancelados. Periodo = mes planeado, vacío = todos los meses pendientes. Estado = estado del artículo.

Solicitado estimado suma cantidad por costo estimado; cotizado usa la primera cotización seleccionada por id (mismo criterio del detalle) con impuestos y cargos; comprometido solo compromisos activos formalizados. No sumar etapas. Redondear cada artículo a centavos antes de agregar para conciliar los importes visibles y Excel. Distinguir sin estimación (total solicitado parcial) de sin precio (sin estimado ni cotización seleccionada). Mostrar cotizado parcial si falta selección. Cero capturado es conocido; null es desconocido.

El desglose cuenta solicitudes distintas, no líneas. Enlaces de departamento conservan periodo y estado. Excel incluye Resumen y Artículos, filtros y alcance, números nativos y campos ausentes en blanco. No exportar fórmulas desde texto de usuarios. Filtros inválidos muestran error y ningún resultado; exportación inválida responde 400.

Archivos: nuevo compras/resumen_departamentales.py, vista existente, template bandeja, CSS específico, pruebas y versión de static/erp-sw.js. Sin modelos, migraciones, permisos ni datos operativos. Validación: PostgreSQL, pruebas del módulo, checks, navegador local/producción y lectura de XLSX. Entrega por PR, merge, deploy oficial y cierre registrado.
