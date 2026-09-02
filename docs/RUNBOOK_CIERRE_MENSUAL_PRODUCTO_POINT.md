# Runbook - Cierre Mensual De Producto Point

## Objetivo

Operar el cierre mensual teorico de producto terminado dentro del ERP sin depender del Excel como herramienta principal.

## Alcance

Incluye:

- inventario inicial teorico
- produccion Point
- ventas Point conciliadas
- conversion de derivadas a entero padre
- merma Point
- cierre mensual teorico

No incluye:

- inventario fisico
- diferencias contra conteo
- ajustes manuales por auditoria fisica

## Roles

- `DG`:
  - ver
  - construir
  - rebuild
  - bloquear
- `ADMIN`:
  - ver
  - construir
  - rebuild
  - bloquear
- `PRODUCCION`:
  - ver
  - construir
- `ALMACEN`:
  - ver
  - construir
- `LECTURA` y otros roles operativos:
  - ver solamente

## Reglas de datos

### Opening del mes

Orden de resolucion:

1. cierre del mes anterior
2. snapshot Point del ultimo dia del mes anterior
3. snapshot Point mas reciente dentro de tolerancia

Tolerancia oficial:

- `3` dias calendario

Si no existe snapshot exacto ni dentro de tolerancia:

- no construir el mes

### Catalogo incompleto

Si existen productos del opening sin homologacion Point -> ERP:

- el cierre si se puede construir
- el cierre no se puede bloquear

Si existen lineas con derivadas sin relacion activa o catalogo pendiente:

- el cierre si se puede construir
- el cierre no se puede bloquear

## Flujo operativo

### Contención del schedule mensual

`setup_celery_schedules` registra `pos_bridge: cierre producto mensual` pausado y
con `lock_after_build=false`. Por tanto, ejecutar el setup durante un deploy no
refresca, reconstruye ni bloquea agosto ni ningún otro periodo. La tarea Celery y
el comando manual permanecen disponibles para una corrida explícita.

Solo una decisión operacional explícita puede habilitar la cadencia:

```bash
./.venv/bin/python manage.py setup_celery_schedules --enable-monthly-product-closure
```

Incluso habilitado, el schedule únicamente construye un borrador revisable; el
bloqueo institucional sigue siendo una acción separada de DG/ADMIN. Una ejecución
posterior de `setup_celery_schedules` sin el flag devuelve el schedule a pausado.

### 1. Vista operativa

Ruta:

- `/reportes/cierre-producto/`

Uso:

- seleccionar mes
- construir si no existe
- revisar excepciones y guardas
- exportar CSV/XLSX si se necesita evidencia
- bloquear solo cuando el mes quede limpio

### 2. Build manual por comando

```bash
./.venv/bin/python manage.py build_product_month_closure \
  --month 2025-09 \
  --actor-username <usuario>
```

Opciones:

- `--rebuild`
- `--lock-after-build`
- `--approval-note`
- `--approval-reason`

### 3. Backfill en rango

Dry-run:

```bash
./.venv/bin/python manage.py backfill_product_month_closure \
  --from-month 2025-09 \
  --to-month 2025-12 \
  --dry-run
```

Ejecucion real:

```bash
./.venv/bin/python manage.py backfill_product_month_closure \
  --from-month 2025-09 \
  --to-month 2025-12 \
  --actor-username <usuario> \
  --approval-reason backfill_inicial
```

## Criterios para bloquear el mes

Se puede bloquear solo si:

- estado `BUILT`
- tiene lineas
- no tiene productos de opening sin homologacion
- no tiene incidencias de catalogo en lineas

El bloqueo deja rastro en `metadata.lock_event` con:

- fecha
- actor
- canal
- motivo
- nota

La revalidación final usa un mutex transaccional por mes y vuelve a calcular la
huella inmediatamente antes de sellar. El backfill oficial de ventas, producción,
merma, conversiones y snapshots de inventario adquieren el mismo advisory mutex
mensual, en orden cronológico cuando abarcan varios meses. Un snapshot coordina
su mes de captura y el siguiente porque puede servir como cierre y como apertura.
No toma locks `SHARE` sobre tablas completas: otros periodos continúan operando.
Los catálogos globales no forman parte de la huella cruda mensual; su efecto se
congela en el digest de líneas proyectadas, evitando prometer serialización que
sus servicios globales no ofrecen.
Las escrituras sobre líneas de un cierre ya bloqueado sí se rechazan por trigger
de base de datos. Las fuentes Point conservan su huella inmutable en el cierre;
una corrección histórica posterior exige un rebuild operacional explícito y un
nuevo bloqueo, no la mutación silenciosa de las líneas selladas.

## Qué revisar antes de bloquear

- opening source correcto
- fecha de opening correcta

En un import histórico, el archivo Excel acredita únicamente las columnas de
inventario/conteo declaradas, preservando por alcance celda vacía frente a cero
explícito. Ventas, producción y merma leídas de tablas
operativas quedan como observaciones no validadas hasta acreditar cobertura; si
falta una fuente, la pantalla, API, CSV y XLSX muestran `Sin dato`/`null`, nunca
un cero autoritativo ni un saldo calculado aparentemente válido.
El saldo inicial histórico se publica como `Saldo inicial`, nunca como inventario
Point. En metadata antigua sólo cuenta como presente si conserva la clave histórica
del alcance; el decimal placeholder por sí solo no acredita disponibilidad.
- venta derivada coherente
- merma derivada coherente
- sin guardas activas en la vista
- sin incidencias de catalogo

## Qué hacer si falla

### Caso 1. No hay snapshot

- revisar si existe snapshot del mes previo
- revisar si la fecha cae fuera de tolerancia
- re-ejecutar sync de inventario si aplica
- si el snapshot historico ya no existe en Point, sembrar un seed controlado:

```bash
./.venv/bin/python manage.py bootstrap_product_month_closure \
  "/Users/mauricioburgos/Downloads/COMPATIVO PRODUCCION - VENTAS ACTUAL.xlsx" \
  --sheet "SEPT 25" \
  --seed-month 2025-08 \
  --quantity-column D \
  --dry-run
```

Luego ejecutar la misma corrida sin `--dry-run` y continuar con `backfill_product_month_closure` desde `2025-09`.

### Caso 2. Producto Point sin homologacion

- homologar receta/codigo Point
- reconstruir el mes si corresponde

### Caso 3. Derivada sin relacion activa

- capturar `RecetaPresentacionDerivada`
- reconstruir el mes

### Caso 4. Mes bloqueado pero detectaron error

- no corregir desde UI
- decidir en gobierno si se habilita rebuild por comando/API
- registrar motivo del rebuild
