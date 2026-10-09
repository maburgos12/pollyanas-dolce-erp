# Ficha de fuentes — protección de merma Point y recuperación original

Fecha/ambiente: 3 octubre 2026, PostgreSQL16 local aislado y VPS read-only.

## Necesidad y unidad de análisis

Una línea documental de merma y su espejo MermaPOS. Evitar interpretar ausencia
en un listado como cancelación. Mauricio autorizó explícitamente la reparación
del importador y la recuperación exclusiva del movimiento1683114, no inventario.

| Fuente | Identificador y ámbito | Creador/consumidores | Evidencia |
| --- | --- | --- | --- |
| PointWasteLine/pos_bridge_waste_lines | PK1676/hash9f27f6de945bbc8b19cd, movimiento1683114 | PointMovementSyncService; auditor y reportes | Backup original y raw78023/79066, 5PZA |
| MermaPOS/control_mermapos | PK1676/mismo hash; receta16/Matriz1 | Espejo importador; reportes | Backup conserva5, fecha27sept |
| PointSyncJob | 78023/79066/80579 | Manifiesto; validador mensual | 267 originales/266 actuales;80579superseded1 |
| Raw y stock history419 | Movimiento1683114 | Point; evidencia guardada | MERMA43→38/no cancelado; listado omite límite inicial |

## Alias y equivalencias

Ambos PK1676 pertenecen a tablas distintas; relación confirmada por source_hash,
movimiento/raw, producto y cantidad. Branch original24 es aliasMatriz, ERP1:
conservarlo; no sustituir por PK10 canónica. Timestamp original guardadoUTC se
conserva literalmente; no imponer desplazamiento7h ni cambiar America/Mazatlan.

## Decisión / diseño aprobado

Extender importador existente, sin tabla ni segunda captura. En consulta usar
margen calendario y filtrar por intervalo operativo solicitado, sin certificar
cobertura por ese margen. Si una sincronización completa pierde hashes existentes,
fallar atómicamente con evidencia limitada: no borrar, no reemplazar parcialmente.
La cancelación real necesita evidencia independiente, no ausencia del listado.

Recuperación acotada: leer solo las dos filas originales desde backup existente,
validar PK/hash/producto/fecha/cantidad, crear staging temporal PostgreSQL y copiar
valores originales completos; colisión diferente aborta ambos. Dry-run por defecto,
aplicación explícita e idempotencia estricta. No HTTP, stock, ventas, avisos,
resúmenes de job ni restauración general del backup. Script publicado por Git.

## Plan de implementación y aceptación

- [ ] TDD pérdida inicial y rollback de todos los upserts; parcial/dedup intactos.
- [ ] TDD margen/filtrado/duplicados/fechas inválidas y cierre de sesión.
- [ ] TDD recuperación dry-run, originales, colisión, corrupción y segundo no-op.
- [ ] Revisar diff/pruebas/check0/migratecheck0 y CI SHAactual completo.
- [ ] Merge/deploy oficial; dry-run recuperación y aplicar exclusivamente el par.
- [ ] Verificar266→267 y evidencia3237 visible sin cambiar ventas477/stock;
      autoridad mensual y documentación siguen evaluándose, no cierre automático.
- [ ] Segundo recovery0 filas/HTTP; revisar producción/auth y limpiar task exacta.

Descubrimiento: grafo identificó métodos pero devolvió rutas incorrectas/snippets
sin fuente; lectura directa del módulo verificado. inventario_fuentes_datos con
--term merma --term waste identificó modelos y unicidad en PostgreSQL local.
VPS: consulta read-only confirmó ausencias, PK libres, branch24/Matriz y receta16.
Backup leído streaming, sin restauración. Los demás registros son fuera de alcance.
