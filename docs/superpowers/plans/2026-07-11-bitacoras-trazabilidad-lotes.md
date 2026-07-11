# Bitacoras de produccion con lotes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar en local el piloto Pastel Crunch Chico para que una bitacora cerrada genere lotes y movimientos trazables desde produccion hasta CFP 1.1 y Armado, sin duplicar movimientos que ya provengan de Point.

**Architecture:** Point conserva la identidad canonica y sus movimientos externos. Las bitacoras son el documento operativo interno; un servicio transaccional e idempotente genera `LoteProduccion` y efectos en `MovimientoInventario`. `ExistenciaInsumo` pasa a ser una proyeccion por insumo y ubicacion, mientras el corte ciego compara el conteo fisico contra movimientos confirmados.

**Tech Stack:** Django 5, PostgreSQL, Django templates, `Decimal`, transacciones y bloqueos `select_for_update`, pruebas `django.test.TestCase`.

---

## Mapa de archivos

**Crear**

- `operacion/bitacoras_config.py`: configuracion canonica de Hornos, CFP 1.1 y Armado.
- `operacion/services_bitacoras_inventory.py`: cierres, lotes, cortes, FIFO y conciliacion Point.
- `inventario/services_existencias.py`: unica puerta para leer y modificar saldos por ubicacion.
- `operacion/migrations/0002_expand_bitacoras_trazables.py`: nuevos tipos y metadatos de cierre/corte.
- `inventario/migrations/0013_existencias_por_ubicacion.py`: unicidad de saldos por ubicacion.
- `inventario/migrations/0014_lotes_y_origen_movimientos.py`: lotes y origen estructurado de movimientos.
- `operacion/migrations/0003_corte_ciego.py`: usuario y momento del corte ciego.

**Modificar**

- `inventario/models.py`: ubicaciones, `LoteProduccion`, existencia por ubicacion y origen trazable.
- `operacion/models.py`: tipos HORNOS/ARMADO y estado del corte ciego.
- `operacion/views.py`: comandos explicitos de guardar existencia, cerrar produccion, entregar y cerrar Armado.
- `operacion/urls.py`: rutas de revision y autorizacion del piloto.
- `templates/operacion/bitacora_captura.html`: estados progresivos y datos ocultos antes del corte.
- `templates/operacion/bitacoras_home.html`: acceso al piloto y pendientes de identidad.
- `static/operacion/sw.js`: bump de cache por cada cambio visible acumulado.
- `pos_bridge/services/movement_sync_service.py`: vincular o conciliar eventos Point sin doble aplicacion.
- `compras/views.py`, `inventario/views.py`, `inventario/utils/almacen_import.py`, `inventario/services_conteo_fisico.py`, `inventario/services_consumo_bom.py`, `recetas/views/plan.py`, `recetas/views/mrp.py`, `recetas/views/reabasto.py`, `inventario/management/commands/backfill_existencias_insumos.py`, `inventario/management/commands/importar_excel_almacen.py`, `pos_bridge/management/commands/sync_inventario_desde_point.py`: pasar ubicacion explicita al servicio de existencias.

**Pruebas**

- `inventario/tests.py`: saldos por ubicacion y regresion de consumidores existentes.
- `operacion/tests.py`: identidad, corte ciego, cierres, FIFO, Armado, permisos e idempotencia.
- `pos_bridge/tests/test_movement_sync_service.py`: precedencia Point/bitacora.

## Task 1: Establecer la linea base limpia y el contrato Point

**Files:**
- Modify: `operacion/tests.py`
- Create: `operacion/bitacoras_config.py`
- Modify: `operacion/models.py`
- Create: `operacion/migrations/0002_expand_bitacoras_trazables.py`

- [ ] **Step 1: Verificar la base antes de escribir codigo**

Run:

```bash
APP_ENV=local DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgres://localhost:5432/pastelerias_bitacoras_lotes \
python3 manage.py migrate
APP_ENV=local DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgres://localhost:5432/pastelerias_bitacoras_lotes \
python3 manage.py migrate --check
APP_ENV=local DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgres://localhost:5432/pastelerias_bitacoras_lotes \
python3 manage.py check
```

Expected: migraciones pendientes `0` y `System check identified no issues`.

- [ ] **Step 2: Escribir la prueba fallida de identidad canonica**

Agregar a `OperacionAppTests`:

```python
def test_hornos_rows_require_point_identity(self):
    user = self._user("piloto.hornos")
    self._grant(user, "produccion")
    self.client.force_login(user)
    receta = Receta.objects.create(
        nombre="Pan Chocolate Chico",
        codigo_point="PAN-CHO-CH",
        tipo=Receta.TIPO_PREPARACION,
        pasa_modulo_produccion=True,
        hash_contenido="piloto-pan-chocolate-ch",
    )

    response = self.client.get("/app/bitacoras/HORNOS/")

    self.assertContains(response, receta.nombre)
    self.assertContains(response, receta.codigo_point)
    self.assertNotContains(response, "Producto inventado")
```

- [ ] **Step 3: Ejecutar la prueba y comprobar RED**

Run:

```bash
python3 manage.py test operacion.tests.OperacionAppTests.test_hornos_rows_require_point_identity
```

Expected: FAIL porque `HORNOS` no existe en la configuracion limpia.

- [ ] **Step 4: Agregar tipos y configuracion minima**

En `operacion/models.py` agregar:

```python
TIPO_HORNOS = "HORNOS"
TIPO_ARMADO = "ARMADO"
```

Incluir ambos en `TIPO_CHOICES`. Crear `operacion/bitacoras_config.py` con el contrato minimo:

```python
from .models import BitacoraOperativa

BITACORA_CONFIG = {
    BitacoraOperativa.TIPO_HORNOS: {
        "titulo": "Control produccion - Hornos",
        "familia": "produccion_lotes",
        "campos": ["existencia", "preparacion"],
        "tipos_receta": ["PREPARACION"],
    },
    BitacoraOperativa.TIPO_CFP11: {
        "titulo": "Inventario CFP 1.1",
        "familia": "custodia_lotes",
        "campos": ["existencia_fisica", "salida_armado"],
    },
    BitacoraOperativa.TIPO_ARMADO: {
        "titulo": "Control produccion - Armado",
        "familia": "transformacion_lotes",
        "campos": ["consumo_real", "producto_terminado"],
        "tipos_receta": ["PRODUCTO_FINAL"],
    },
}
```

Actualizar `operacion/views.py` para importar esta configuracion en vez del diccionario local. La nueva configuracion debe conservar `SALIDAS_CFP1`, `INVENTARIO_CFP1`, `PLAGAS`, `ROTACION` y `REBANADO`; solo agrega Hornos y Armado y especializa CFP 1.1. Resolver filas primero por `codigo_point` y no crear objetos desde texto capturado.

- [ ] **Step 5: Generar migracion y comprobar GREEN**

Run:

```bash
python3 manage.py makemigrations operacion --name expand_bitacoras_trazables
python3 manage.py test operacion.tests.OperacionAppTests.test_hornos_rows_require_point_identity
python3 manage.py migrate --check
```

Expected: PASS y ninguna migracion adicional sin generar.

- [ ] **Step 6: Commit**

```bash
git add operacion/models.py operacion/bitacoras_config.py operacion/views.py operacion/tests.py operacion/migrations/0002_expand_bitacoras_trazables.py
git commit -m "feat: establecer identidad Point en bitacoras de produccion"
```

## Task 2: Habilitar existencias por ubicacion sin romper consumidores

**Files:**
- Modify: `inventario/models.py`
- Create: `inventario/services_existencias.py`
- Create: `inventario/migrations/0013_existencias_por_ubicacion.py`
- Modify: archivos consumidores listados en el mapa
- Test: `inventario/tests.py`

- [ ] **Step 1: Escribir pruebas fallidas para dos ubicaciones**

```python
def test_same_insumo_has_independent_stock_per_location(self):
    insumo = Insumo.objects.create(nombre="Relleno Crunch", activo=True)
    cfp = get_or_create_existencia(insumo, "CFP_1_1")
    armado = get_or_create_existencia(insumo, "ARMADO")
    aplicar_delta(insumo, "CFP_1_1", Decimal("8"))
    aplicar_delta(insumo, "ARMADO", Decimal("2"))

    cfp.refresh_from_db()
    armado.refresh_from_db()
    self.assertEqual(cfp.stock_actual, Decimal("8"))
    self.assertEqual(armado.stock_actual, Decimal("2"))
```

```python
def test_apply_delta_rejects_negative_result(self):
    insumo = Insumo.objects.create(nombre="Pan Chocolate Chico", activo=True)
    aplicar_delta(insumo, "CFP_1_1", Decimal("2"))
    with self.assertRaises(ValidationError):
        aplicar_delta(insumo, "CFP_1_1", Decimal("-3"))
```

- [ ] **Step 2: Ejecutar pruebas y comprobar RED**

Run:

```bash
python3 manage.py test inventario.tests.InventarioUbicacionTests
```

Expected: ERROR de importacion porque el servicio no existe y la relacion sigue siendo `OneToOneField`.

- [ ] **Step 3: Implementar modelo y servicio minimo**

En `inventario/models.py`:

```python
UBICACION_CFP_1_1 = "CFP_1_1"
UBICACION_ARMADO = "ARMADO"
UBICACION_CFP_1 = "CFP_1"

class ExistenciaInsumo(models.Model):
    insumo = models.ForeignKey(Insumo, on_delete=models.CASCADE, related_name="existencias")
    almacen = models.CharField(max_length=20, choices=ALMACEN_CHOICES, default="ALMACEN_1", db_index=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["insumo", "almacen"], name="uniq_existencia_insumo_almacen")
        ]
```

En `inventario/services_existencias.py`:

```python
from decimal import Decimal
from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from .models import ExistenciaInsumo


def get_or_create_existencia(insumo, almacen="ALMACEN_1"):
    return ExistenciaInsumo.objects.get_or_create(insumo=insumo, almacen=almacen)[0]


@transaction.atomic
def aplicar_delta(insumo, almacen, delta):
    existencia, _ = ExistenciaInsumo.objects.select_for_update().get_or_create(
        insumo=insumo,
        almacen=almacen,
    )
    nuevo = Decimal(str(existencia.stock_actual or 0)) + Decimal(str(delta))
    if nuevo < 0:
        raise ValidationError("Existencia insuficiente en la ubicacion seleccionada.")
    existencia.stock_actual = nuevo
    existencia.actualizado_en = timezone.now()
    existencia.save(update_fields=["stock_actual", "actualizado_en"])
    return existencia
```

- [ ] **Step 4: Migrar consumidores existentes a ubicacion explicita**

Reemplazar cada `ExistenciaInsumo.objects.get_or_create(insumo=...)` de los archivos listados en el mapa por `get_or_create_existencia(insumo, almacen)`. Cuando el flujo no tenga ubicacion, usar `ALMACEN_1` para preservar comportamiento. En `PointMovementSyncService`, usar `CUARTO_FRIO` o la ubicacion derivada del movimiento Point, nunca el default implicito.

- [ ] **Step 5: Generar migracion y ejecutar regresion**

Run:

```bash
python3 manage.py makemigrations inventario --name existencias_por_ubicacion
python3 manage.py migrate
python3 manage.py test inventario.tests.InventarioUbicacionTests pos_bridge.tests.test_movement_sync_service compras.tests
python3 manage.py check
```

Expected: pruebas PASS y cero errores de sistema.

- [ ] **Step 6: Commit**

```bash
git add inventario compras recetas pos_bridge api
git commit -m "feat: separar existencias de insumos por ubicacion"
```

## Task 3: Crear lotes y origen trazable de movimientos

**Files:**
- Modify: `inventario/models.py`
- Create: `inventario/migrations/0014_lotes_y_origen_movimientos.py`
- Test: `inventario/tests.py`

- [ ] **Step 1: Escribir prueba fallida del lote determinista**

```python
def test_lote_code_uses_point_code_date_and_source_line(self):
    lote = LoteProduccion.objects.create(
        insumo=self.insumo,
        receta=self.receta,
        cantidad_inicial=Decimal("8"),
        unidad=self.kg,
        producido_en=timezone.make_aware(datetime(2026, 7, 11, 8, 30)),
        linea_origen=self.linea,
        creado_por=self.user,
    )
    self.assertEqual(lote.codigo, f"LOT-RC-001-20260711-{self.linea.id}")
```

- [ ] **Step 2: Ejecutar y comprobar RED**

Run: `python3 manage.py test inventario.tests.LoteProduccionTests`

Expected: FAIL porque `LoteProduccion` no existe.

- [ ] **Step 3: Implementar `LoteProduccion` y vinculos**

```python
class LoteProduccion(models.Model):
    DISPONIBLE = "DISPONIBLE"
    AGOTADO = "AGOTADO"
    RETENIDO = "RETENIDO"
    CANCELADO = "CANCELADO"

    codigo = models.CharField(max_length=120, unique=True, editable=False)
    insumo = models.ForeignKey(Insumo, null=True, blank=True, on_delete=models.PROTECT, related_name="lotes_produccion")
    receta = models.ForeignKey("recetas.Receta", on_delete=models.PROTECT, related_name="lotes_produccion")
    cantidad_inicial = models.DecimalField(max_digits=18, decimal_places=3)
    unidad = models.ForeignKey("maestros.UnidadMedida", on_delete=models.PROTECT)
    producido_en = models.DateTimeField()
    linea_origen = models.OneToOneField(
        "operacion.BitacoraOperativaLinea",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="lote_generado",
    )
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    estado = models.CharField(max_length=16, default=DISPONIBLE)
    es_apertura = models.BooleanField(default=False)
    observaciones = models.CharField(max_length=255, blank=True, default="")
```

Normalizar `codigo_point` a mayusculas alfanumericas y guiones. Generar el codigo despues de tener `linea_origen_id`; no usar una secuencia global mutable.

Agregar a `MovimientoInventario`:

```python
lote = models.ForeignKey(LoteProduccion, null=True, blank=True, on_delete=models.PROTECT, related_name="movimientos")
linea_bitacora = models.ForeignKey(
    "operacion.BitacoraOperativaLinea",
    null=True,
    blank=True,
    on_delete=models.PROTECT,
    related_name="movimientos_inventario",
)
registrado_por_usuario = models.ForeignKey(
    settings.AUTH_USER_MODEL,
    null=True,
    blank=True,
    on_delete=models.SET_NULL,
)
trazabilidad = models.JSONField(default=dict, blank=True)
```

Una preparacion interna requiere `insumo` y `receta`. Un producto terminado requiere `receta` de tipo `PRODUCTO_FINAL` y puede dejar `insumo` vacio porque su saldo se registra con `MovimientoProductoCedis` e `InventarioCedisProducto`. Un lote ordinario requiere `linea_origen`; un lote con `es_apertura=True` exige observacion y no pretende conocer un origen historico.

- [ ] **Step 4: Ejecutar pruebas y migraciones**

Run:

```bash
python3 manage.py makemigrations inventario --name lotes_y_origen_movimientos
python3 manage.py migrate
python3 manage.py test inventario.tests.LoteProduccionTests
python3 manage.py migrate --check
```

Expected: PASS y cero migraciones sin generar.

- [ ] **Step 5: Commit**

```bash
git add inventario/models.py inventario/migrations inventario/tests.py
git commit -m "feat: agregar lotes y origen de movimientos"
```

## Task 4: Registrar la apertura inicial sin inventar historia

**Files:**
- Create: `operacion/services_bitacoras_inventory.py`
- Modify: `operacion/views.py`
- Modify: `operacion/urls.py`
- Modify: `templates/operacion/bitacoras_home.html`
- Test: `operacion/tests.py`

- [ ] **Step 1: Escribir prueba fallida de apertura**

```python
def test_manager_creates_initial_lot_without_fake_source(self):
    lote = registrar_apertura_inicial(
        receta=self.receta,
        insumo=self.insumo,
        cantidad=Decimal("3"),
        unidad=self.kg,
        ubicacion="CFP_1_1",
        fecha_elaboracion=None,
        actor=self.manager,
        observaciones="Existencia previa al inicio de trazabilidad",
    )
    self.assertTrue(lote.es_apertura)
    self.assertIsNone(lote.linea_origen)
    self.assertTrue(lote.codigo.startswith("INI-RC-001-"))
    self.assertEqual(stock_ubicacion(self.insumo, "CFP_1_1"), Decimal("3"))
```

- [ ] **Step 2: Ejecutar y comprobar RED**

Run: `python3 manage.py test operacion.tests.AperturaInicialLotesTests`

Expected: ERROR porque `registrar_apertura_inicial` no existe.

- [ ] **Step 3: Implementar apertura restringida**

Crear `registrar_apertura_inicial(...)` con `transaction.atomic`. Exigir acceso `manage` a Produccion, cantidad positiva, identidad Point y observacion no vacia. Crear primero el lote, asignar codigo `INI-{codigo Point}-{fecha operativa}-{id lote}` y generar una entrada idempotente en la ubicacion seleccionada.

- [ ] **Step 4: Exponer una sola pantalla de apertura**

Agregar ruta `bitacoras/apertura/`. Mostrar productos canonicos, cantidad, unidad, ubicacion, fecha conocida opcional y observacion. No permitir editar una apertura aplicada; cualquier correccion usa el flujo compensatorio de Task 10.

- [ ] **Step 5: Ejecutar pruebas y commit**

Run: `python3 manage.py test operacion.tests.AperturaInicialLotesTests`

Expected: PASS.

```bash
git add operacion/services_bitacoras_inventory.py operacion/views.py operacion/urls.py operacion/tests.py templates/operacion/bitacoras_home.html
git commit -m "feat: registrar apertura inicial de lotes"
```

## Task 5: Cerrar Hornos de forma atomica e idempotente

**Files:**
- Modify: `operacion/services_bitacoras_inventory.py`
- Modify: `operacion/views.py`
- Test: `operacion/tests.py`

- [ ] **Step 1: Escribir pruebas fallidas de cierre e idempotencia**

```python
def test_close_hornos_creates_one_lot_and_cfp_entry(self):
    result = cerrar_hornos(self.bitacora, self.user)
    self.assertEqual(result.lotes_creados, 1)
    lote = LoteProduccion.objects.get(linea_origen=self.linea)
    movimiento = MovimientoInventario.objects.get(linea_bitacora=self.linea)
    self.assertEqual(movimiento.lote, lote)
    self.assertEqual(movimiento.tipo, MovimientoInventario.TIPO_ENTRADA)
    self.assertEqual(movimiento.almacen, "CFP_1_1")

def test_close_hornos_twice_does_not_duplicate(self):
    cerrar_hornos(self.bitacora, self.user)
    cerrar_hornos(self.bitacora, self.user)
    self.assertEqual(LoteProduccion.objects.filter(linea_origen=self.linea).count(), 1)
    self.assertEqual(MovimientoInventario.objects.filter(linea_bitacora=self.linea).count(), 1)
```

- [ ] **Step 2: Ejecutar y comprobar RED**

Run: `python3 manage.py test operacion.tests.HornosLoteServiceTests`

Expected: ERROR porque `cerrar_hornos` no existe.

- [ ] **Step 3: Implementar cierre minimo**

En `operacion/services_bitacoras_inventory.py` implementar `cerrar_hornos(bitacora, actor)` con `transaction.atomic`, `select_for_update` sobre bitacora y existencias, y estas validaciones:

```python
if bitacora.tipo != BitacoraOperativa.TIPO_HORNOS:
    raise ValidationError("La bitacora no corresponde a Hornos.")
if linea.receta is None or not linea.receta.codigo_point:
    raise ValidationError("La linea requiere una receta con codigo Point.")
if linea.receta.rendimiento_unidad_id is None:
    raise ValidationError("La preparacion requiere unidad de rendimiento.")
```

Usar `source_hash = sha256(f"BITACORA:HORNOS:{linea.id}:ENTRADA:CFP_1_1".encode()).hexdigest()`. Crear lote y movimiento con `get_or_create`; aplicar delta solo cuando el movimiento sea nuevo. Cerrar la bitacora al final de la misma transaccion.

- [ ] **Step 4: Conectar la accion POST**

En `bitacora_captura`, aceptar `accion=cerrar_produccion`. El guardado ordinario solo crea o actualiza borrador. El cierre llama `cerrar_hornos` y vuelve a la misma URL con `revision=<id>`.

- [ ] **Step 5: Ejecutar pruebas**

Run:

```bash
python3 manage.py test operacion.tests.HornosLoteServiceTests operacion.tests.OperacionAppTests
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add operacion/services_bitacoras_inventory.py operacion/views.py operacion/tests.py
git commit -m "feat: generar lotes al cerrar Hornos"
```

## Task 6: Implementar corte ciego matutino CFP 1.1

**Files:**
- Modify: `operacion/models.py`
- Create: `operacion/migrations/0003_corte_ciego.py`
- Modify: `operacion/views.py`
- Modify: `templates/operacion/bitacora_captura.html`
- Modify: `static/operacion/sw.js`
- Test: `operacion/tests.py`

- [ ] **Step 1: Escribir prueba de ocultamiento y revelado**

```python
def test_cfp_count_hides_expected_until_saved(self):
    response = self.client.get("/app/bitacoras/CFP11/")
    self.assertNotContains(response, "Existencia esperada")
    self.assertNotContains(response, "Stock fijo")

    response = self.client.post(
        "/app/bitacoras/CFP11/",
        {"accion": "guardar_existencia", "existencia_fisica_0": "5"},
        follow=True,
    )
    self.assertContains(response, "Existencia esperada")
    self.assertContains(response, "Diferencia")
```

- [ ] **Step 2: Ejecutar y comprobar RED**

Run: `python3 manage.py test operacion.tests.CfpBlindCountTests`

Expected: FAIL porque la plantilla actual muestra un formulario generico y no conserva revision.

- [ ] **Step 3: Implementar la conciliacion**

Agregar a `BitacoraOperativa`:

```python
conteo_guardado_en = models.DateTimeField(null=True, blank=True)
conteo_guardado_por = models.ForeignKey(
    settings.AUTH_USER_MODEL,
    null=True,
    blank=True,
    on_delete=models.SET_NULL,
    related_name="cortes_ciegos_guardados",
)
```

La funcion `guardar_corte_ciego(bitacora, actor)` debe guardar cantidades fisicas y despues calcular por insumo:

```python
esperado = saldo_movimientos_hasta(insumo, "CFP_1_1", bitacora.conteo_guardado_en)
diferencia = existencia_fisica - esperado
```

Antes de `conteo_guardado_en`, el contexto no debe incluir esperado, lotes, stock ni proyeccion. Despues debe incluirlos y registrar hora/usuario.

- [ ] **Step 4: Notificar solo diferencias**

Reutilizar `crear_notificaciones`. Enviar una notificacion por corte con resumen de productos diferentes de cero; no enviar por lineas `OK`.

- [ ] **Step 5: Bump PWA y validar**

Cambiar `CACHE_NAME` en `static/operacion/sw.js` a `pollyanas-app-operativa-pwa-v21-corte-ciego-lotes`.

Run:

```bash
python3 manage.py test operacion.tests.CfpBlindCountTests
python3 manage.py check
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add operacion/models.py operacion/migrations operacion/views.py operacion/tests.py templates/operacion/bitacora_captura.html static/operacion/sw.js
git commit -m "feat: agregar corte ciego matutino en CFP 1.1"
```

## Task 7: Transferir lotes FIFO de CFP 1.1 a Armado

**Files:**
- Modify: `operacion/services_bitacoras_inventory.py`
- Modify: `operacion/views.py`
- Modify: `templates/operacion/bitacora_captura.html`
- Test: `operacion/tests.py`

- [ ] **Step 1: Escribir pruebas FIFO e inventario insuficiente**

```python
def test_transfer_uses_oldest_available_lot_first(self):
    result = entregar_a_armado(self.insumo, Decimal("6"), self.linea, self.user)
    self.assertEqual(result.asignaciones, [(self.lote_antiguo.id, Decimal("5")), (self.lote_nuevo.id, Decimal("1"))])
    self.assertEqual(stock_ubicacion(self.insumo, "CFP_1_1"), Decimal("2"))
    self.assertEqual(stock_ubicacion(self.insumo, "ARMADO"), Decimal("6"))

def test_transfer_rejects_more_than_available(self):
    with self.assertRaises(ValidationError):
        entregar_a_armado(self.insumo, Decimal("9"), self.linea, self.user)
```

- [ ] **Step 2: Ejecutar y comprobar RED**

Run: `python3 manage.py test operacion.tests.CfpFifoTransferTests`

Expected: ERROR porque `entregar_a_armado` no existe.

- [ ] **Step 3: Implementar asignacion FIFO atomica**

Ordenar lotes `DISPONIBLE` por `producido_en`, `id`; bloquearlos con `select_for_update`. Derivar disponibilidad por lote desde movimientos. Por cada asignacion crear:

```python
SALIDA  lote X, almacen="CFP_1_1"
ENTRADA lote X, almacen="ARMADO"
```

Usar hashes distintos y deterministas para ambas patas. Si la suma disponible es menor que la solicitud, no crear ningun movimiento.

- [ ] **Step 4: Implementar excepcion FIFO auditada**

Aceptar lote manual solo cuando `motivo_excepcion_fifo` no este vacio y el usuario tenga acceso `manage` a Produccion. Guardar motivo en `notas` de ambos movimientos.

- [ ] **Step 5: Ejecutar pruebas y commit**

Run: `python3 manage.py test operacion.tests.CfpFifoTransferTests`

Expected: PASS.

```bash
git add operacion/services_bitacoras_inventory.py operacion/views.py operacion/tests.py templates/operacion/bitacora_captura.html
git commit -m "feat: transferir lotes FIFO de CFP a Armado"
```

## Task 8: Consumir lotes y generar Pastel Crunch Chico

**Files:**
- Modify: `operacion/services_bitacoras_inventory.py`
- Modify: `operacion/views.py`
- Modify: `templates/operacion/bitacora_captura.html`
- Test: `operacion/tests.py`

- [ ] **Step 1: Escribir prueba de consumo real contra receta**

```python
def test_close_armado_consumes_real_and_creates_finished_lot(self):
    result = cerrar_armado(self.bitacora, self.user)
    self.assertEqual(result.producto.codigo_point, self.crunch_ch.codigo_point)
    self.assertEqual(result.cantidad_terminada, Decimal("8"))
    self.assertEqual(result.consumo_real[self.relleno.id], Decimal("5.5"))
    self.assertEqual(result.consumo_teorico[self.relleno.id], Decimal("4.8"))
    self.assertTrue(result.lote_terminado.codigo.startswith(f"LOT-{self.crunch_ch.codigo_point}"))
```

- [ ] **Step 2: Ejecutar y comprobar RED**

Run: `python3 manage.py test operacion.tests.ArmadoCrunchPilotTests`

Expected: ERROR porque `cerrar_armado` no existe.

- [ ] **Step 3: Implementar cierre de Armado**

Validar que la receta producto final:

- tenga `codigo_point`;
- tenga lineas principales con `insumo` canonico;
- tenga cantidad y unidad compatibles;
- tenga lotes suficientes recibidos en Armado.

El consumo teorico se calcula `cantidad_terminada * linea.cantidad`; el consumo real proviene de la bitacora. Crear movimientos `CONSUMO` por lote usando el real. El producto terminado es la `Receta` canonica de Point; no crear un `Insumo` duplicado para representarlo. Generar el lote terminado con la receta Point, `insumo=None`, y crear su entrada mediante `MovimientoProductoCedis` e `InventarioCedisProducto`.

- [ ] **Step 4: Bloquear receta incompleta**

Agregar prueba que una linea sin insumo canonico permite guardar borrador pero hace que `cerrar_armado` lance `ValidationError("La receta tiene componentes pendientes de vinculacion.")`.

- [ ] **Step 5: Ejecutar pruebas y commit**

Run:

```bash
python3 manage.py test operacion.tests.ArmadoCrunchPilotTests recetas.tests.RecetaDerivedInsumoAutolinkTests
```

Expected: PASS.

```bash
git add operacion/services_bitacoras_inventory.py operacion/views.py operacion/tests.py templates/operacion/bitacora_captura.html
git commit -m "feat: cerrar piloto Crunch con consumo real y lote terminado"
```

## Task 9: Evitar doble aplicacion entre Point y bitacoras

**Files:**
- Modify: `pos_bridge/services/movement_sync_service.py`
- Modify: `operacion/services_bitacoras_inventory.py`
- Test: `pos_bridge/tests/test_movement_sync_service.py`
- Test: `operacion/tests.py`

- [ ] **Step 1: Escribir prueba de precedencia**

```python
def test_point_sync_links_matching_bitacora_movement_without_second_delta(self):
    bitacora_movement = self.create_bitacora_entry(quantity=Decimal("8"), point_code="RC-001")
    before = stock_ubicacion(bitacora_movement.insumo, "CFP_1_1")

    self.service.run_production_sync(lines=[self.point_line(quantity=Decimal("8"), point_code="RC-001")])

    after = stock_ubicacion(bitacora_movement.insumo, "CFP_1_1")
    self.assertEqual(after, before)
    self.assertEqual(MovimientoInventario.objects.filter(insumo=bitacora_movement.insumo).count(), 1)
```

- [ ] **Step 2: Ejecutar y comprobar RED**

Run: `python3 manage.py test pos_bridge.tests.test_movement_sync_service.PointMovementSyncServiceTests.test_point_sync_links_matching_bitacora_movement_without_second_delta`

Expected: FAIL porque el sincronizador aplica una entrada adicional.

- [ ] **Step 3: Implementar conciliacion conservadora**

Antes de crear una entrada Point, buscar un movimiento de bitacora no conciliado con:

- mismo `codigo_point` canonico;
- misma fecha operativa;
- misma cantidad y unidad;
- misma ubicacion destino.

Si existe una sola coincidencia exacta, conservar un solo movimiento y guardar `point_source_hash`, `point_external_id` y `conciliado_en` en `MovimientoInventario.trazabilidad`. Si hay cero coincidencias, aplicar Point normalmente. Si hay mas de una o difieren cantidades, crear `PointPendingMatch` con metodo `BITACORA_MOVEMENT_RECONCILIATION` y no modificar el movimiento de bitacora.

- [ ] **Step 4: Probar los tres estados**

Agregar casos para coincidencia exacta, Point sin bitacora y discrepancia. Ejecutar:

```bash
python3 manage.py test pos_bridge.tests.test_movement_sync_service operacion.tests
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pos_bridge/services/movement_sync_service.py pos_bridge/tests/test_movement_sync_service.py operacion/services_bitacoras_inventory.py operacion/tests.py
git commit -m "feat: conciliar movimientos Point y bitacoras sin duplicar stock"
```

## Task 10: Permisos, ajustes y auditoria

**Files:**
- Modify: `operacion/views.py`
- Modify: `operacion/urls.py`
- Modify: `operacion/services_bitacoras_inventory.py`
- Modify: `templates/operacion/bitacora_captura.html`
- Test: `operacion/tests.py`

- [ ] **Step 1: Escribir pruebas de autorizacion**

```python
def test_employee_cannot_edit_closed_bitacora(self):
    response = self.client.post(self.url, {"accion": "corregir", "cantidad": "7"})
    self.assertEqual(response.status_code, 403)

def test_manager_correction_creates_compensating_movement(self):
    self.client.force_login(self.manager)
    response = self.client.post(self.url, {"accion": "corregir", "cantidad": "7", "motivo": "Conteo verificado"})
    self.assertEqual(response.status_code, 302)
    self.original.refresh_from_db()
    self.assertEqual(self.original.cantidad, Decimal("8"))
    self.assertTrue(MovimientoInventario.objects.filter(referencia__startswith=f"AJUSTE:{self.original.id}").exists())
```

- [ ] **Step 2: Ejecutar y comprobar RED**

Run: `python3 manage.py test operacion.tests.BitacoraCorrectionPermissionTests`

Expected: FAIL porque no existe accion de correccion autorizada.

- [ ] **Step 3: Implementar correccion compensatoria**

Autorizar solo a usuarios con acceso `manage` a Produccion. Exigir motivo no vacio. No actualizar cantidad original. Crear movimiento compensatorio con referencia `AJUSTE:{movimiento_original.id}:{linea.id}`, usuario y fecha.

- [ ] **Step 4: Ejecutar pruebas y commit**

Run: `python3 manage.py test operacion.tests.BitacoraCorrectionPermissionTests`

Expected: PASS.

```bash
git add operacion/views.py operacion/urls.py operacion/services_bitacoras_inventory.py operacion/tests.py templates/operacion/bitacora_captura.html
git commit -m "feat: autorizar correcciones trazables de bitacoras"
```

## Task 11: Validar UI responsive y cierre local completo

**Files:**
- Modify: `templates/operacion/bitacora_captura.html`
- Modify: `templates/operacion/bitacoras_home.html`
- Modify: `static/operacion/sw.js`
- Test: `operacion/tests.py`

- [ ] **Step 1: Completar pruebas de contenido responsive**

Agregar aserciones para:

```python
self.assertContains(response, 'data-capture-state="blind"')
self.assertContains(response, 'data-layout="mobile-line"')
self.assertContains(response, 'data-layout="tablet-grid"')
self.assertContains(response, 'data-layout="desktop-table"')
self.assertNotContains(response, "Día de captura")
```

- [ ] **Step 2: Ejecutar suite del modulo**

Run:

```bash
python3 manage.py test operacion inventario.tests pos_bridge.tests.test_movement_sync_service
python3 manage.py check
python3 manage.py migrate --check
```

Expected: todas las pruebas PASS, cero errores y cero migraciones pendientes.

- [ ] **Step 3: Iniciar servidor local**

Run:

```bash
APP_ENV=local DEBUG=1 WEB_HOST_PORT=8014 CANONICAL_LOCAL_HOST=127.0.0.1:8014 \
ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgres://localhost:5432/pastelerias_bitacoras_lotes \
python3 manage.py runserver 127.0.0.1:8014
```

Expected: servidor disponible en `http://127.0.0.1:8014/app/bitacoras/`.

- [ ] **Step 4: Validar el recorrido en navegador**

Validar con usuario local de Produccion:

1. Hornos en 390x844, 768x1024 y 1365x912.
2. Cierre genera un solo lote visible.
3. CFP oculta esperado antes del corte y lo revela despues.
4. Entrega FIFO aparece en Armado sin recaptura.
5. Armado genera lote terminado Crunch CH.
6. No hay errores de consola ni solicitudes 4xx/5xx inesperadas.
7. El corte del dia siguiente muestra `OK`, `Falta` o `Sobra` correctamente.

- [ ] **Step 5: Ejecutar diff y estado final**

Run:

```bash
git diff --check
git status --short --branch
git log --oneline --decorate -12
```

Expected: solo cambios del piloto en la rama y ningun artefacto temporal.

- [ ] **Step 6: Commit de cierre local**

```bash
git add templates/operacion/bitacora_captura.html templates/operacion/bitacoras_home.html static/operacion/sw.js operacion/tests.py
git commit -m "test: validar piloto local de trazabilidad por lotes"
```

## Condiciones para extender el piloto

No extender a otros productos hasta cumplir simultaneamente:

- Pastel Crunch Chico cuadra de Hornos a CFP 1.1, Armado e Inventario CFP1.
- Un cierre repetido no duplica lotes ni stock.
- Point y bitacora no duplican el mismo movimiento.
- La diferencia del corte siguiente identifica correctamente faltante o sobrante.
- Una correccion conserva el movimiento original.
- Todas las pruebas y validaciones responsive pasan en local.
- Mauricio revisa la UI local y aprueba el flujo.

No hacer push, PR ni deploy durante este plan sin autorizacion explicita posterior.
