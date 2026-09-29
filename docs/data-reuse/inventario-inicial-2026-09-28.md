# Inventario inicial de fuentes del ERP

Ambiente observado: PostgreSQL 16 local aislado, creado desde las migraciones de `main` el 28 de septiembre de 2026. Tiene esquema y algunos registros iniciales creados por migraciones, pero no datos operativos de producción. El conteo del catálogo PostgreSQL es de 375 tablas base y 779 relaciones de llave foránea. Estos números describen este corte del código; no son un inventario de producción ni una prueba de registros duplicados.

Lectura adicional de producción, 28 de septiembre de 2026 (America/Mazatlan), mediante una transacción `READ ONLY`: `core_sucursal` 12 filas; `rrhh_empleado` 97; `maestros_insumo` 627; `maestros_insumoalias` 247; `maestros_proveedor` 124; `mantenimiento_proveedorservicio` 37. Solo se consultaron conteos. Las 12 filas de sucursal no equivalen a 12 tiendas activas y las dos tablas de proveedores no prueban registros duplicados; requieren revisar estados, ámbitos e identificadores.

| Concepto de búsqueda | Candidatos hallados | Lectura inicial |
| --- | --- | --- |
| Insumo / material | `maestros.Insumo`, `maestros.InsumoAlias`, `inventario.ExistenciaInsumo`, `inventario.ConsumoInsumoMensual`, `pos_bridge.PointInsumoInventorySnapshot` | El maestro, los alias, las existencias, el consumo y el snapshot de Point tienen funciones diferentes; las relaciones apuntan a `maestros.Insumo`. |
| Empleado / colaborador | `rrhh.Empleado`, `bonos_produccion.BonoProduccionEmpleado`, `bonos_ventas.BonoVentasEmpleado`, `rrhh.AsistenciaEmpleado` | Los registros de bono y asistencia se relacionan con `rrhh.Empleado`. Hace falta revisar las identidades externas por fuente antes de importar personal. |
| Sucursal / tienda | `core.Sucursal`, `horarios_especiales.SucursalAlias`, tablas de ventas, inventario y reportes con referencia a `core.Sucursal` | El alias y las referencias permiten encontrar sucursales por diversos nombres; los históricos y ámbitos se revisan por separado. |
| Proveedor | `maestros.Proveedor`, `mantenimiento.ProveedorServicio`, compras y activos con referencia a `maestros.Proveedor` | `ProveedorServicio` es un candidato a comparar con `Proveedor`; no se presume que ambos catálogos tengan el mismo ámbito ni que sus filas sean equivalentes. |

## Decisiones pendientes antes de cualquier unión de registros

- Definir, por dominio, identificadores estables y ámbitos: por ejemplo, código Point para insumos, códigos de empleado por fuente y códigos de sucursal con vigencia histórica.
- Revisar ejemplos positivos, negativos y ambiguos de la base operativa con consultas limitadas de solo lectura. Este inventario local no puede confirmar equivalencias entre filas.
- Determinar qué fuente crea y actualiza cada atributo cuando dos sistemas ofrecen valores diferentes.
- Documentar consumidores y efectos antes de cambiar alias, llaves o capturas.

Para repetir la búsqueda: `python3 manage.py inventario_fuentes_datos --term <concepto> --term <sinónimo> --presence`. El resultado es JSON e incluye modelos, columnas, unicidad, relaciones, tablas sin modelo vigente y presencia de filas. `--presence` solo devuelve un booleano por tabla; no extrae valores. `--details` amplía la información de campos.
