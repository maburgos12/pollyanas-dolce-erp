# Cotizaciones y compra realizada Implementation Plan

**Goal:** Permitir corregir cotizaciones con evidencia y documentar compras antes de la entrega.
**Architecture:** Formularios Django y servicios transaccionales bajo Compras; historial JSON y registro de compra único por artículo. Reutilizar flujo de órdenes y autorizaciones.
**Tech Stack:** Django, PostgreSQL 16, templates existentes y data-async-action.

- [x] Inspeccionar modelos, cotización, órdenes, recepción, permisos y consumidores.
- [x] Crear worktree registrado, PostgreSQL aislado 55565, aplicar base y verificar check/migrate.
- [x] Implementar historial y compra en compras/models.py y migración aditiva.
- [x] Formularios, control de versión, validación de estados y servicio transaccional; revisar los valores anterior/nuevo y compromisos.
- [x] Integrar detalle y formularios con data-async-action, descargas autorizadas, bump ERP SW y cobertura UX.
- [x] Escribir pruebas en compras/tests_edicion_compra.py: comparar sin alterar selección, monto incrementado solicita DG, compra única sin crear recepción, errores/permisos y archivos.
- [x] Ejecutar módulo Compras, check, migrate --check y makemigrations --check en DB aislada.
- [x] Validar formularios, errores, historial, compra y recepción en navegador con datos locales; inspeccionar consola y requests.
- [ ] Revisar diff, commit específico, PR borrador y CI completa; merge y deploy oficial con verificaciones producción de solo lectura.
- [ ] Cerrar worktree y ramas mediante ciclo de vida, dejar evidencia de resultado y límites.
