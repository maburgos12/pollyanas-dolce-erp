# Acciones sin perder contexto

## Decisión

El ERP adoptará un contrato progresivo y opt-in para acciones que cambian estado. No se harán reemplazos globales ciegos. Cada pantalla migrada conservará su POST tradicional y añadirá mejora asíncrona sobre el mismo endpoint y la misma lógica de negocio.

## Contrato compartido

- `templates/base.html` alojará una región global de toast con `aria-live`, sin mover el layout ni robar foco.
- `static/js/erp_actions.js` interceptará solo formularios con `data-async-action`. Bloqueará únicamente el botón usado, conservará el texto de inputs ante error y rechazará un segundo envío mientras el primero siga activo.
- El backend devolverá JSON cuando `Accept: application/json` esté presente y continuará usando redirect para HTML.
- Todo formulario mutante migrado tendrá `data-context-anchor` y un `context_anchor` oculto. El fallback HTML redirigirá al elemento afectado mediante fragmento.
- Los errores importantes producirán toast persistente; éxitos, advertencias e información cerrarán automáticamente entre 4 y 5 segundos.
- Las confirmaciones modales compartidas se reservarán para acciones destructivas o irreversibles y deberán restaurar foco, admitir Escape y atrapar foco.

## Piloto

`logistica:ruta_detail`, sección `#revisiones-entrega`, migrará Autorizar, Rechazar y Marcar corregida. La respuesta JSON incluirá el estado y la presentación de la revisión afectada. El navegador reemplazará únicamente esa fila; si falla, mantendrá el motivo capturado y permitirá reintentar. Sin JavaScript, el POST seguirá funcionando y volverá a `#revision-entrega-<id>`.

## Compatibilidad

No cambian permisos, estados, servicios, auditoría, endpoints públicos ni reglas de cierre. Django Messages seguirá disponible para el fallback HTML, pero en pantallas migradas se renderizará como toast global.

## Etapas

1. Infraestructura compartida y piloto Logística.
2. Acciones de aprobación y rechazo de RRHH, seguimiento y compras.
3. Guardados frecuentes en inventario, recetas, bonos, mantenimiento y activos.
4. Resto del inventario, cerrando explícitamente pendientes y excepciones.

Cada etapa actualizará `docs/ux/action-context-coverage.md` con pantalla, acción, fallback, prueba y estado.

