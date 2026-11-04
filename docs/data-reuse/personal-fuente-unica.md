# Personal: diagnóstico y corrección propuesta

2026-11-04. Consultas acotadas en PostgreSQL de producción con SET TRANSACTION READ ONLY. Sin cambios operativos en esta revisión.

## Problema verificado
- Asignación por sucursal llama /api/rrhh/empleados/?area=VENTAS y filtra Empleado.area, no departamento.
- Sol Alejandra Mascareño Romero (19), Colosio, y Fernanda Michelle Garay Castro (22), El Túnel, tienen departamento VENTAS, función CAJAS y sucursal_ref correcta, pero area PRODUCCION. Por eso la pantalla omite a una persona en cada sucursal.
- Matriz devuelve 10 con ese filtro antiguo: 8 del departamento Ventas más Minerva Cecilia Cota Medina (26, Hornos) y Guadalupe Valdez Soberanes (30, Envío a sucursales). Excluye a Ruth Santos (area CAJAS), 5 repartidores y Johana (area ADMINISTRACION).
- Departamento VENTAS activo en Matriz: 15 personas = 7 cajas/auxiliar/encargado + Perla Lizbeth Valenzuela Inzunza (37, sin puesto específico) + Fernanda call center + 5 repartidores + Johana. En nómina octubre son 14: Perla no figura en periodos 22/24. Esto requiere mostrar plantilla vigente y nómina histórica por separado, no inventar un costo.
- Plantilla actual del departamento Ventas por sucursal: Matriz15, Bamoa2, Colosio2, El Túnel2, Guamúchil3, Leyva3, Payán2, Plaza Las Glorias2, Plaza Nío2, Sinaloa de Leyva3. Total36; no equivale a 36 cajeras.
- Bonos octubre incluye 37 personas distintas por sucursal, con Minerva y Guadalupe de Producción en Matriz y Perla sin línea de nómina de octubre. La selección usa participa_bonos_ventas OR áreas heredadas; aparecer en bonos no acredita puesto Cajas. No recalcular ni quitar bonos capturados.
- Organización presenta 54 advertencias y 4 informaciones. 44 advertencias son propuestas de alineación de área con función; 5 adicionales de repartidores. Es un dry-run, no una certificación de plantilla ni una mutación ejecutada. Cambiar área=VENTAS a CAJAS sin corregir consumidores omitiría más personas del filtro antiguo.
- Yesenia(3), Carolina(7) y Johana(8) tienen jefe_directo vacío. Mauricio ya existe como Empleado89, Director General, pero activo=False; Paula33 ya lo tiene como jefe. La consulta de EmpleadoBaja no devuelve baja para89. La fecha/causa de su inactivación no quedó determinada; no reactivar, duplicar persona ni generar nómina por inferencia.
- Ya existen normalizar_nombre y Empleado._resolver_funcion_operativa: normalizan acentos, mayúsculas, espacios y separadores para reconocer catálogo. No todos los consumidores utilizan los mismos campos. El problema no se resuelve poniendo todos los textos en mayúsculas.

## Fuente maestra y contratos
- Persona: rrhh.Empleado por id/codigo estable.
- Sucursal vigente: sucursal_ref; no emparejar por textos.
- Departamento vigente: departamento; función: puesto_operativo + CatalogoFuncionOperativa; nivel y jefe_directo independientes.
- Nómina histórica: NominaLinea.sucursal_snapshot/departamento_snapshot y periodo; no reemplazar con ubicación vigente.
- Bonos: BonoVentasEmpleado conserva sus periodos, importes, ajustes y decisiones. Sus registros son evidencia de pertenencia al periodo, no catálogo de puesto ni autorización para recalcular.

## Solución recomendada y alcance revisable
1. Corregir selección de plantilla en Asignación para usar departamento y sucursal por ID, mostrando función y excluyendo bajas. Conservar filtro area de API para consumidores existentes; añadir filtro explícito departamento o reutilizar equivalente existente tras revisar callers.
2. Unificar presentación de función desde catálogo en Empleados/Organización/Asignación/reporte. Distinguir puesto específico pendiente de departamento/sucursal conocidos. No asignar CAJAS a todas las fichas vacías ni inferir cargo de grupos de acceso.
3. Registrar jefe_directo=89 en Yesenia/Carolina/Johana según instrucción expresa de Mauricio, con auditoría antes/después. Mostrar dirección organizacional ya existente sin reactivar ficha laboral ni ampliar permisos. Validar consumidores que filtran jefes activos antes de aplicar.
4. Corregir diagnóstico de Organización para no marcar inconsistencia sólo por departamento amplio versus función específica. Mantener advertencias reales y exponer motivo, no cambiar severidad masivamente a Info.
5. Reporte octubre: venta, personal del periodo, percepciones ambas quincenas, prestaciones documentadas, subtotal y porcentaje por sucursal. Presentar puesto pendiente dentro de Ventas con etiqueta precisa y no afirmar costo de Cajas definitivo. Presentar producción, limpieza, administración/jefaturas, logística/reparto, almacén/cuartos fríos y call center separados; reconciliar cada peso al total. Conservar faltantes de cargas/provisiones y no afirmar costo integral.
6. Bonos: verificar selección para nuevos periodos con la misma fuente de identidad; separar el indicador de participación de la función. Cambios de elegibilidad o recalculo histórico requieren decisión específica, no aplicar silenciosamente.

Archivos/consumidores previstos: api/rrhh_views.py y serializer, rrhh/views.py, rrhh/views_html.py, servicios/catálogos de RRHH, templates de Empleados/Organización/Asignación, reportes/services_personal_registrado.py, views/templates/exportaciones de Planeación, bonos_ventas/empleados.py sólo si revisión demuestra cambio autorizado de elegibilidad. Pruebas de API/listados/organización/reportes, permisos, contratos existentes, anti-duplicación y conservación de nómina y ajustes manuales. Bump SW si corresponde.

Riesgos: alterar elegibilidad de bonos por equiparar participación y función; contar plantilla vigente como histórica; reactivar director como empleado de nómina; recalcular pagos; trasladar costos de otros centros a Matriz. Contener mediante cambios aditivos y pruebas con los casos reales identificados.

Estado: alcance aprobado por Mauricio el 2026-11-04. Implementación en worktree personal-fuente-unica y PostgreSQL16 sintético exclusivo erp_personal_fuente_unica, puerto55539. Inventario léxico empleado/sucursal ejecutado; consultas reales corroboran la ficha. Las modificaciones no cambian elegibilidad ni pagos de bonos: se añade diagnóstico explícito del conflicto. No se crean tablas ni una segunda ficha de persona.
