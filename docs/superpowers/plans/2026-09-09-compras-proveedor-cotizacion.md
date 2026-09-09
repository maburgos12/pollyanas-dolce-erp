# Proveedor en cotización — Implementation Plan

> Ejecutar en el worktree registrado compras-proveedor-cotizacion. Diseño aprobado en conversación; aplicar executing-plans y revisión independiente antes de merge.

**Goal:** Cotizar con un proveedor nuevo o vendedor online sin abandonar la solicitud.
**Architecture:** Formulario Django validado; alta explícita en maestros.Proveedor desde endpoint de compras con el permiso existente; reemplazo HTML del selector mediante ERPActionUI. Plataforma y URL pertenecen a la cotización. La relación al proveedor y servicios presupuestales se conservan.
**Tech Stack:** Django 5, PostgreSQL 16, templates y contrato data-async-action existente.

- [x] Crear compras/tests_proveedor_cotizacion.py: POST alta con nombre normalizado, duplicado/inactivo 409, auditoría, acceso denegado; POST cotización online guarda enlace/plataforma; URL javascript y cantidad inválida producen 400 sin filas; guardar sin seleccionar conserva presupuesto; errores tradicionales devuelven inputs.
- [x] Ejecutar el módulo de pruebas y corroborar fallos contra la funcionalidad ausente.
- [x] Crear compras/forms_cotizaciones.py con ModelForm de cotización, URL limitada a HTTP(S), requerimiento de enlace online, cantidad positiva y costos no negativos. ProveedorForm valida nombre y plazo; servicio de alta en transacción bloquea catálogo y rechaza duplicados normalizados sin modificar inactivos.
- [x] Añadir plataforma y enlace_producto a CotizacionCompraDepartamental, generar migración con manage.py makemigrations compras, aplicar y verificar migrate --check.
- [x] Adaptar views_departamentales.py y urls.py: alta auditada, respuesta target/html sin recarga, fallback con fragmento; cotización validada/transaccional, error JSON o HTML ligado al artículo.
- [x] Separar cotizacion_form.html y proveedor_cotizacion.html; selector externo asociado al form de cotización por id, alta como form hermano, detalles progresivos, selector de plataforma y enlace, errores visibles y valores preservados. Mostrar evidencia en detalle; CSS scoped, bump ERP SW y registro de cobertura.
- [x] Ejecutar manage.py test compras.tests_proveedor_cotizacion compras.tests_departamentales compras.tests_envio_departamental compras.tests_resumen_departamentales --keepdb --noinput; check y makemigrations --check --dry-run. Esperado: sin fallos ni cambios pendientes.
- [x] Validar navegador local con fixtures identificados: alta, selección, captura preservada, error/reintento, online, comparación y selección; escritorio/móvil, consola y Network.
- [ ] Revisar diff independiente, corregir hallazgos, commit quirúrgico y PR borrador. Esperar CI completo; merge y deploy_web_safe.sh; verificar migración, UI autenticada, caché. Cerrar con task_workspace_close.sh y auditoría.

Evidencia local: 48 pruebas aprobadas; check/migrate --check/makemigrations --check limpios. Navegador autenticado confirmó alta y selección sin perder precio, observaciones ni PDF, duplicado 409, enlace online obligatorio 400 y guardado 200; comparación conserva selección y estado anteriores. Vista 390 px con scrollWidth=390; consola sin errores. Revisión independiente sin hallazgos.
