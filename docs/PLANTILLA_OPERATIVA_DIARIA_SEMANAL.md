# Plantilla Operativa Diario/Semanal (Go Live)

Fecha de inicio: ____ / ____ / ______  
Semana: ______________________  
Coordinador operativo: ______________________

## 1. Roles y Responsables
- `ADMIN ERP`: control de usuarios, aprobaciones, auditoria, escalamiento.
- `ALMACEN`: inventario diario, entradas/salidas/merma, sync y aliases.
- `COMPRAS`: solicitudes, ordenes, recepciones, presupuesto.
- `VENTAS/PLANEACION`: solicitud de produccion, validacion contra pronostico.
- `DIRECCION`: revision de KPIs ejecutivos y decisiones.

## 2. Rutina Diaria (Hora sugerida)
- `08:30` ALMACEN: ejecutar sync (`Carga Almacen`) o carga XLSX manual del dia.
- `08:45` ALMACEN: revisar `Inventario > Alertas` y `Inventario > Aliases`.
- `09:00` COMPRAS: revisar `Compras > Solicitudes` y prioridad (`Criticos` / `Bajo reorden`).
- `09:30` COMPRAS: convertir solicitudes aprobadas a ordenes y enviar a proveedor.
- `12:00` ALMACEN: registrar recepciones parciales/totales y validar stock actualizado.
- `16:30` VENTAS/PLANEACION: validar plan del siguiente dia vs consumo real.
- `17:00` ADMIN: revisar bitacora (`Bitacora`) y cerrar incidencias del dia.

## 3. Checklist Diario (marcar)
- [ ] Sync/carga almacen ejecutado sin error.
- [ ] Pendientes de match criticos revisados.
- [ ] Solicitudes criticas atendidas.
- [ ] Ordenes emitidas del dia.
- [ ] Recepciones capturadas y stock actualizado.
- [ ] Reporte rapido de desviacion revisado.
- [ ] Incidencias registradas en bitacora.

## 4. Rutina Semanal (Corte sugerido: Viernes 17:00)
- Consolidar consumo real vs plan (`Compras > Presupuesto y Desviacion`).
- Revisar top insumos con desvio y definir acciones.
- Revisar pendientes de homologacion (Point + Almacen + Recetas).
- Depurar aliases duplicados o incorrectos.
- Auditar proveedores: cumplimiento, retrasos, incidencias.
- Confirmar que no haya alertas rojas sin responsable.

## 5. Checklist Semanal (marcar)
- [ ] Homologacion de nombres al dia (`pendientes criticos = 0`).
- [ ] Presupuesto semanal validado vs ejecutado.
- [ ] Top 10 insumos con mayor desviacion revisados.
- [ ] Flujo completo probado: solicitud -> orden -> recepcion -> inventario.
- [ ] Reportes CSV/XLSX generados y archivados.
- [ ] Acciones correctivas asignadas con responsable y fecha.

## 6. Semaforo Operativo
- `VERDE`: sin errores 500, pendientes criticos = 0, flujo diario completo.
- `AMARILLO`: pendientes criticos <= 10 o 1 modulo con atraso.
- `ROJO`: errores 500 recurrentes, pendientes criticos > 10, ordenes sin recepcion > 48h.

## 7. Regla de Escalamiento
- Escalar a `ADMIN ERP` si:
- Error 500 en modulo productivo.
- Desfase de inventario critico.
- Importacion/sync fallida dos veces consecutivas.
- Escalar a `DIRECCION` si:
- Variacion de costo semanal > umbral definido.
- Falta de insumos criticos para produccion del siguiente dia.

## 8. Cierre Diario (Resumen)
- Fecha: ______________________
- Estado semaforo: `VERDE / AMARILLO / ROJO`
- Incidencias abiertas: _______
- Acciones para manana:
- 1. ______________________
- 2. ______________________
- 3. ______________________

## 9. Cierre Semanal (Resumen Ejecutivo)
- Semana: ______________________
- Presupuesto estimado: ______________________
- Gasto real: ______________________
- Variacion: ______________________
- Principales causas:
- 1. ______________________
- 2. ______________________
- 3. ______________________
- Decisiones acordadas:
- 1. ______________________
- 2. ______________________
- 3. ______________________
