# Origen conservador de conversiones Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Conservar las entradas de conversión confirmadas por Point sin inventar el producto origen y conciliar todos los movimientos bajo la sucursal ERP real.

**Architecture:** Los servicios de balance seguirán leyendo las tablas canónicas existentes. La trazabilidad agregará aliases `PointBranch` por `erp_branch_id` antes de calcular casos, elegirá una sucursal Point representativa para persistencia y mantendrá como incidencia el origen ausente. La interfaz sólo presentará una causa breve; la evidencia continuará disponible en el detalle progresivo.

**Tech Stack:** Django 5, PostgreSQL 16, plantillas Django, CSS existente y pruebas `django.test.TestCase`.

---

### Task 1: Conversión sin origen explícito

**Files:**
- Modify: `pos_bridge/tests/test_monthly_product_balance_service.py`
- Modify: `pos_bridge/tests/test_branch_inventory_traceability_service.py`
- Modify: `pos_bridge/services/monthly_product_balance_service.py`
- Modify: `pos_bridge/services/branch_inventory_traceability_service.py`

- [x] Cambiar primero las pruebas para exigir que la entrada destino permanezca y que la salida del padre sea cero cuando Point no informa origen.
- [x] Ejecutar las pruebas puntuales y comprobar el fallo por la resta inferida existente.
- [x] Eliminar únicamente la inferencia de origen basada en equivalencia; conservar la equivalencia como validación cuando Point sí informa origen.
- [x] Ejecutar nuevamente las pruebas puntuales y la suite de ambos servicios.

### Task 2: Sucursal ERP canónica

**Files:**
- Modify: `pos_bridge/tests/test_branch_inventory_traceability_service.py`
- Modify: `pos_bridge/services/branch_inventory_traceability_service.py`

- [x] Agregar una prueba donde cierre y venta usan el alias numérico y producción/conversión el alias nominal de la misma sucursal ERP.
- [x] Comprobar que la prueba falla porque el servicio genera ubicaciones separadas.
- [x] Agregar la normalización por `erp_branch_id`, fusionando cantidades, IDs e impactos sin modificar las filas fuente.
- [x] Verificar una sola línea por producto y sucursal ERP, sin falso `SOURCE_INCOMPLETE`.

### Task 3: Lectura clara de incidencias

**Files:**
- Modify: `reportes/tests_inventory_traceability_views.py`
- Modify: `reportes/views_inventory_traceability.py`

- [x] Agregar pruebas de las etiquetas “Origen de conversión por identificar” y “Transferencia por conciliar”.
- [x] Comprobar el fallo con las etiquetas actuales.
- [x] Mapear los códigos de incidencia a esas etiquetas breves, manteniendo la evidencia en el detalle.
- [x] Verificar la tabla y el detalle accesible mediante las pruebas de vista existentes.

### Task 4: Verificación, integración y producción

**Files:**
- Create: `docs/data-reuse/2026-09-29-auditoria-conversiones-inventario.md`
- Modify: `static/erp-sw.js` sólo si cambia un recurso servido por su caché.

- [x] Ejecutar `python3 manage.py test` para los servicios y vistas afectados.
- [x] Ejecutar `python3 manage.py check` y `python3 manage.py migrate --check` con PostgreSQL.
- [ ] Revisar el diff, confirmar que no hay descargas ni tablas nuevas y crear commit/PR.
- [ ] Mergear, desplegar mediante `scripts/deploy_web_safe.sh`, reconstruir agosto usando las filas existentes y validar en producción 3 Pecados Chico, Mediano y Rebanada.
