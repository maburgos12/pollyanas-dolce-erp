# Resumen departamental Implementation Plan

**Goal:** consultar el importe pendiente entre departamentos y descargar el mismo desglose.
**Architecture:** formulario GET y cálculo compartido para HTML/XLSX en compras/resumen_departamentales.py; vista conserva autorización actual.
**Tech Stack:** Django, PostgreSQL 16, templates y openpyxl existentes.

- [ ] Crear compras/tests_resumen_departamentales.py: exclusiones por solicitud/artículo, importes de cotizaciones y compromisos, nulos y cero, agrupación sin duplicados, filtros combinados e inválidos, acceso y XLSX sin fórmulas.
- [ ] Ejecutar pruebas y confirmar fallos por funcionalidad ausente.
- [ ] Implementar formulario, selección de pendientes y cálculo único. Añadir exportación en la misma ruta con exportar=xlsx.
- [ ] Integrar filtros, cuatro indicadores, advertencias de cobertura, tabla departamental y detalle con importes. Conservar navegación y acciones de consulta.
- [ ] Agregar CSS limitado a la bandeja y subir versión del service worker ERP.
- [ ] Ejecutar tests de compras, check y migrate --check. Revisar diff y navegador local con datos sintéticos.
- [ ] Commit quirúrgico, PR borrador, CI, revisión, merge, migrate --check en VPS y deploy_web_safe.sh.
- [ ] Validar producción y exportación, cerrar worktree con script oficial y auditar.
