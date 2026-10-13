# Conciliación nocturna de conteos Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Conciliar observaciones físicas al cierre con evidencia Point, conservando captura independiente y pendientes explícitos.
**Architecture:** Hora propia por lectura; cálculo por intervalos sobre historial canónico; tarea nocturna que publica resultados aparte de referencia manual con control de versión. Reutilizar fuentes y cliente Point existentes sin modificar saldos.
**Tech Stack:** Django 5, PostgreSQL 16, Celery/Beat, templates y JS existentes, biblioteca estándar.

Ejecución dentro de este hilo y del worktree registrado; autorización de alcance ya recibida. No crear otra conversación ni pedir nuevamente autorización del mismo alcance.

## 1. Hora por lectura y conservación
Files: inventario/models_conteos.py, services_conteos.py, views_conteos.py; migración nueva; tests_conteos_domain.py, tests_conteos_views.py.
- [x] Prueba de captura con hora declarada y guardado posterior de notas: instante permanece, última edición cambia; cliente sin campo sigue enviando.
- [x] Campos aditivos:
```python
# LecturaConteoSucursal
contado_en = models.DateTimeField(null=True, blank=True)
# ConteoSucursal
conciliacion = models.JSONField(default=dict, blank=True)
# EventoConteoSucursal: actor nullable para acción exclusiva del sistema,
# constraint: actor no nulo OR action='conciliacion_nocturna'.
```
- [x] Admitir contado_en opcional en lecturas; interpretar fecha local en America/Mazatlan, validar tipo/tamaño, preservar hora si cantidad no cambia y cliente no la envía. Hora ausente/fuera de ronda/futura no debe fabricar un instante ni impedir guardar cantidad válida.
- [x] Añadir hora al snapshot de envío; limpiar conciliación en reconteo; no tocar cantidades anteriores. Crear/aplicar migración, check y migrate --check.

## 2. Reloj y formulario móvil
Files: templates/inventario/conteos/_linea_captura.html, _detalle.html, detalle.html; static/inventario/conteos.js, conteos.css.
- [x] Campo datetime-local editable, zona horaria visible; hora inicial propuesta al registrar cantidad, calibrada con servidor. No rellenar horas de borradores/cantidades antiguos solo por abrir la página.
- [x] Incluir contado_en en values, persistencia, FormData y lecturas_json; preservar valores y mensajes al rebind. Fallback sin JS mantiene contrato compatible.
- [x] Prueba en navegador: cantidad → hora, corrección de hora, notas, agregar artículo, guardar, revisar y enviar; conservar datos. Cambios de reloj que impidan propuesta segura dejan hora para declaración manual; no bloquean cantidad.

## 3. Cálculo con evidencia
Files: crear inventario/conteos_conciliacion.py y tests_conteos_conciliacion.py; conteos_point.py para resolver sucursal reutilizando la regla existente.
- [x] Escribir prueba numérica:
```text
Contado=3 a20:00. Venta20:15=-1; entrada20:30=+2.
Cierre00:00. Venta00:15=-1; snapshot00:30=3.
Físico al cierre=4; Point al cierre=4; diferencia=0.
La venta00:15 se conserva separada como posterior al cierre.
```
- [x] Calcular con Decimal e intervalos de día operativo; invertir sentido para lecturas/snapshots posteriores al corte. No consultar fuente hasta resolver identidad, cantidad/hora y unidad.
- [x] Reutilizar validación de respuesta original, fechas UTC Point, deduplicación y cancelación de merma. Comprobar cobertura/capacidad, continuidad, paridad del snapshot y ausencia de movimiento en ventana de extracción. Identificar movimientos de físico y posteriores al cierre separadamente.
- [x] Pruebas de cero explícito y ausencia, respuesta parcial/truncada/corrupta, duplicados divergentes, unidad/identidad incorrecta, saldo discontinuo, cancelado/ajuste no acreditado, hora ambigua y físico negativo: pendiente con motivo, sin sustituir datos por cero.

## 4. Consulta y tarea nocturna
Files: conteos_conciliacion.py, inventario/tasks.py, config/settings.py; tests_conteos_conciliacion.py.
- [x] Un cliente Point por lote con lock no bloqueante; escribir archivo original inmutable y usar ingest_original_response con identidad, SHA, localizador, recibo y lector real.
- [x] Ejecutar consulta fuera del lock del conteo; guardar bajo comparación de estado/version/huella. Evento automático actor Sistema; no cambiar version de captura ni datos físicos.
- [x] Tarea diaria02:00 America/Mazatlan; reintentar disponibilidad/atraso en30min, máximo16 intentos. Casos sin hora/insumo no acreditado requieren revisión, sin espera de red.
- [x] Probar Point fallando, ocupado, sincronización tardía, reconteo concurrente y repetición; captura válida siempre funciona sin referencia.

## 5. Revisión, exportación y entrega
Files: views_conteos.py, _detalle.html, CSS, SW; pruebas de vistas/export; operacion/tests.py y tests_higiene.py.
- [x] Mostrar original, físico al cierre, Point al cierre, diferencia y motivo pendiente. Reference manual separada; conservar regla15min. No exponer Point durante captura/reconteo.
- [x] Añadir columnas a Excel sin cambiar columnas existentes y conservar eventos/lecturas previas; Sistema visible en eventos automáticos.
- [x] Bump SW y sus tres assertions. Check, migrate --check, tests del módulo/fuentes y navegador móvil320/390/412 y escritorio.
- [ ] Revisar diff completo; commit quirúrgico; PR borrador, CI completo, merge y deploy oficial; validar perfil empleado, estado real y preservación de datos. No simular actividad en producción.
- [ ] Retirar únicamente entorno temporal de esta tarea por helper con respaldo verificado; cerrar worktree/ramas y registrar recursos/espacio real.
