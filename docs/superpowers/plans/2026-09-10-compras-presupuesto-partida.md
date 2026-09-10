# Presupuesto por partida en compras departamentales

**Objetivo aprobado:** no atribuir el presupuesto total del área a un artículo sin partida. Mostrar «Sin presupuesto asignado» y «No calculable» cuando no exista base válida; enviar nuevas cotizaciones a DG y conservar autorizaciones existentes.

**Arquitectura:** usar el rubro opcional ya existente del artículo, validando pertenencia al área y vigencia. Evaluación explícita de ausencia, distinta de cero. No migraciones ni modificaciones de datos operativos. Compartir evaluación con detalle y bandeja DG.

**Stack:** Django, PostgreSQL 16, templates existentes y service worker ERP.

## Implementación y evidencia

- [x] Worktree registrado y limpio desde origin/main; PostgreSQL aislado 55566, todas las migraciones aplicadas y check correcto.
- [x] Regresiones: falta de rubro, rubro inactivo/otra área/sin línea mensual, prioridad revisado sobre original, presupuesto cero válido, real desconocido, compromisos exclusivos del rubro y exclusión del artículo actual.
- [x] Filtrar presupuesto por rubro activo del artículo y periodo; no sumar área completa. No convertir real desconocido en cero.
- [x] Nuevas selecciones no calculables requieren DG; lectura no altera autorizaciones/órdenes. Ediciones de texto y reducciones ya autorizadas conservan autorización cuando no hay exceso conocido; aumentos siguen requiriendo DG.
- [x] Panel muestra partida, ausencia o disponible calculado con alcance claro; DG explica motivo de revisión; caché actualizada.
- [x] Pruebas de Compras, check, migrate --check, makemigrations --check; revisión de especificación y calidad.
- [x] Navegador local: ausencia con millones ajenos, cotización a DG, autorización y preservación; consola/red y móvil.
- [ ] Commit quirúrgico, PR, CI completo, merge, despliegue oficial y navegador de producción sin escrituras operativas.
- [ ] Cierre oficial del worktree y auditoría final.

Validación local: 180 pruebas de Compras correctas; check sin errores y migraciones sin cambios ni pendientes. Navegador: presupuesto ajeno de $4,320,543.51 desaparece; importe $10,345.00 y autorización previa conservados; nueva selección a DG y autorización explícita correctas. Bandeja DG muestra motivo. POST decisión y GET detalle 200, consola sin errores; móvil 390 px sin desbordamiento. Revisión independiente de especificación y calidad sin hallazgos.
