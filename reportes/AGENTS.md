# Reportes y fuentes financieras

Leer también [AGENTS raíz](../AGENTS.md).

### Presupuesto vs real — trampas de doble conteo (confirmadas en prod)
Antes de conectar cualquier rubro nuevo a una fuente en
`reportes/data/mapeo_rubros_fuentes.csv`, verificar estas dos trampas:
- **Filas AGREGADO del Excel de admón:** la hoja de administración del Excel trae
  conceptos que son la SUMA de las hojas por sucursal (casos confirmados: "Luz 65%",
  "Adquisición de equipo/maquinaria" $278k, servicios #1054, "Etiquetas, bolsas,
  cajas y empaques" rubro 794). Señal típica: su ppto mensual = suma de los rubros
  por sucursal (a veces menos una constante = la sucursal que la hoja vieja no
  incluía) y/o su real legado = suma exacta de capturas por centro. Esos rubros se
  DESACTIVAN (con motivo en metadata), no se conectan — sumarlos duplica contra los
  renglones por sucursal.
- **CONSUMO_MP fuera de producción duplica:** los insumos tipo EMPAQUE (y cualquier
  otro con movimientos) generan filas en `ConsumoInsumoMensual`, y el renglón Costos
  del P&L (`CONSUMO_MP total_empresa`) ya las suma. La exclusión anti-doble-conteo
  de `estado_resultados` (`_es_materia_prima` + rubros con regla CONSUMO_MP) SOLO
  aplica al área `produccion` — conectar rubros de admón o gastos-venta a CONSUMO_MP
  duplica a nivel empresa. Para esos rubros usar GASTO_OPERATIVO por categoría/centro
  (patrón servicios/empaques, PRs #1050-#1054 y #1075).
