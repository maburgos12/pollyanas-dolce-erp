# Compras Departamentales Cancelación y Reembolso Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Permitir cancelar un intento de compra no entregado, seguir reembolsos de compras pagadas, volver a comprar con otro proveedor y cerrar entregas únicamente después de la confirmación del área.

**Architecture:** Introducir un intento de compra explícito que comienza al generar la orden y agrupa cotización, línea, compra, compromiso, cancelación y reembolsos. Un artículo puede conservar varios intentos históricos, pero solo uno vigente; los pagos pendientes de reembolso permanecen como exposición presupuestal separada mientras un reemplazo recorre su propia autorización. Las acciones se implementan en servicios transaccionales y las vistas conservan el contrato progresivo `data-async-action`.

**Tech Stack:** Django 5.0.1, PostgreSQL 16, Django templates, JavaScript/CSS existentes, `openpyxl`, pruebas `django.test`, Docker Compose local.

---

## Límites de ejecución

- Trabajar desde un worktree limpio creado con `scripts/task_workspace_start.sh`; nunca desde el checkout raíz.
- Antes de editar, levantar PostgreSQL 16 aislado, aplicar todas las migraciones de `origin/main` y comprobar `migrate --check` y `check`.
- No modificar compras, reembolsos ni compromisos de producción durante desarrollo o pruebas.
- No enviar correos ni WhatsApp reales. Las pruebas de avisos usan mocks y la migración no crea avisos.
- El cambio incluye modelos, migración y contratos compartidos. Requiere PR, revisión completa, merge, despliegue oficial y validación autenticada en producción.
- Si una compra existente no puede clasificarse con los datos persistidos, la migración conserva el estado más prudente y nunca inventa una cancelación o un reembolso.

## Estructura de archivos

| Archivo | Responsabilidad |
| --- | --- |
| `compras/models.py` | Intentos, reembolsos, restricciones, propiedades de acceso y transición de recepción. |
| `compras/migrations/0016_intentos_compra_reembolsos.py` | Crear contratos, migrar líneas/compras/compromisos existentes y aplicar restricciones finales. |
| `compras/services_intentos_compra.py` | Cancelación, solicitud/confirmación de reembolso y consultas del intento vigente. |
| `compras/services_departamentales.py` | Crear intentos al ordenar, compromisos por intento y confirmación de recepción. |
| `compras/services_edicion_compra.py` | Registrar/corregir una compra dentro del intento vigente. |
| `compras/forms_intentos_compra.py` | Formularios de cancelación y recepción de reembolso. |
| `compras/views_intentos_compra.py` | Endpoints protegidos y respuestas progresivas para ambas acciones. |
| `compras/views_departamentales.py` | Precarga, contexto y etiquetas claras de entrega/confirmación. |
| `compras/urls.py` | Rutas de cancelación y reembolso. |
| `compras/templates/compras/departamentales/detalle.html` | Línea de tiempo, responsables y acciones del intento. |
| `static/css/compras_departamentales.css` | Estados y paneles de intento/reembolso accesibles. |
| `compras/resumen_departamentales.py` | Totales HTML/XLSX separados por naturaleza. |
| `compras/templates/compras/departamentales/bandeja.html` | KPI de reembolso pendiente. |
| `compras/tests_intentos_compra.py` | Pruebas de dominio, permisos, concurrencia, UI y migración funcional. |
| `compras/tests_resumen_departamentales.py` | Pruebas de totales y exportación. |
| `compras/tests_edicion_compra.py` | Regresión de edición, compra y recepción existentes. |
| `compras/tests_avisos_compra.py` | Regresión de avisos vinculados a compras históricas. |
| `docs/ux/action-context-coverage.md` | Registrar cobertura de las nuevas acciones con conservación de contexto. |

### Task 1: Modelar intentos y reembolsos con migración conservadora

**Files:**
- Modify: `compras/models.py:434-538`
- Create: `compras/migrations/0016_intentos_compra_reembolsos.py`
- Create: `compras/tests_intentos_compra.py`

- [ ] **Step 1: Escribir pruebas fallidas del contrato de datos**

Crear una base reutilizable en `compras/tests_intentos_compra.py` a partir del fixture existente, sin depender de datos productivos:

```python
from decimal import Decimal
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import IntegrityError, transaction
from django.test import TestCase
from django.utils import timezone

from compras.models import (
    CompraRealizadaDepartamental, CompromisoCompraDepartamental,
    IntentoCompraDepartamental, RecepcionItemDepartamental,
    ReembolsoCompraDepartamental,
)
from compras.services_departamentales import generar_ordenes_departamentales
from compras.services_edicion_compra import registrar_compra_realizada
from compras.tests_edicion_compra import _CompraDepartamentalBase
from maestros.models import Proveedor


class CompraDepartamentalIntentoBase(_CompraDepartamentalBase, TestCase):
    def setUp(self):
        super().setUp()
        self.compras = self.user
        self.cotizacion = self.quote
        self.otro_proveedor = Proveedor.objects.create(nombre="Proveedor sustituto")

    def crear_intento(self, *, estado="VIGENTE", proveedor=None):
        cotizacion = self.cotizacion
        if proveedor is not None:
            cotizacion = self.item.cotizaciones.create(
                proveedor=proveedor, cantidad_ofertada=self.item.cantidad,
                costo_unitario=Decimal("125.00"), seleccionada=estado == "VIGENTE",
            )
        return IntentoCompraDepartamental.objects.create(
            item=self.item, cotizacion=cotizacion, estado=estado,
        )

    def crear_intento_pagado(self, *, estado="VIGENTE", reembolso_solicitado=None):
        intento = self.crear_intento(estado=estado)
        CompraRealizadaDepartamental.objects.create(
            intento=intento, item=self.item, cotizacion=intento.cotizacion,
            fecha_compra=timezone.localdate(), importe_final=Decimal("1000.00"),
            comprobante=SimpleUploadedFile("compra.pdf", b"%PDF-1.4\n%%EOF"),
            registrado_por=self.compras,
        )
        CompromisoCompraDepartamental.objects.create(
            intento=intento, item=self.item, cotizacion=intento.cotizacion,
            monto=Decimal("1000.00"), activo=True, formalizado_en=timezone.now(),
        )
        if reembolso_solicitado is not None:
            intento.reembolso_solicitado = reembolso_solicitado
            intento.reembolso_solicitado_en = timezone.localdate()
            intento.save(update_fields=["reembolso_solicitado", "reembolso_solicitado_en"])
        return intento

    def ordenar(self):
        generar_ordenes_departamentales([self.item], actor=self.compras)
        self.item.refresh_from_db()
        return self.item.intento_vigente

    def comprar(self, *, importe=Decimal("200.00")):
        intento = self.item.intento_vigente or self.ordenar()
        self.cotizacion.refresh_from_db()
        registrar_compra_realizada(
            self.item, fecha_compra=timezone.localdate(), importe_final=importe,
            numero_pedido="PEDIDO-PRUEBA",
            comprobante=SimpleUploadedFile("compra.pdf", b"%PDF-1.4\n%%EOF"),
            actor=self.compras, cotizacion_id=self.cotizacion.pk,
            version=self.cotizacion.version,
        )
        intento.refresh_from_db()
        return intento

    def recibir(self, intento, cantidad):
        return RecepcionItemDepartamental.objects.create(
            linea_orden=intento.linea_orden, cantidad_recibida=cantidad,
            observaciones="Recepción de prueba", registrado_por=self.compras,
        )

    def intento_con_reembolso_pendiente(self, importe):
        return self.crear_intento_pagado(
            estado=IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO,
            reembolso_solicitado=importe,
        )
```

Añadir estas pruebas y extender el fixture en la misma tarea con helpers que llamen a los servicios reales de orden/compra cuando cada tarea los introduzca:

```python
class IntentoCompraModelTests(CompraDepartamentalIntentoBase):
    def test_un_articulo_conserva_intentos_historicos_y_un_solo_vigente(self):
        primero = self.crear_intento(estado=IntentoCompraDepartamental.ESTADO_CANCELADO_SIN_PAGO)
        segundo = self.crear_intento(estado=IntentoCompraDepartamental.ESTADO_VIGENTE)
        self.assertEqual(self.item.intentos_compra.count(), 2)
        self.assertEqual(self.item.intento_vigente, segundo)
        self.assertNotEqual(primero, segundo)

    def test_no_admite_dos_intentos_vigentes(self):
        self.crear_intento(estado=IntentoCompraDepartamental.ESTADO_VIGENTE)
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                self.crear_intento(estado=IntentoCompraDepartamental.ESTADO_VIGENTE)

    def test_reembolso_acumulado_y_saldo_no_se_vuelven_compra_negativa(self):
        intento = self.crear_intento_pagado(
            estado=IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO,
            reembolso_solicitado=Decimal("1000.00"),
        )
        ReembolsoCompraDepartamental.objects.create(
            intento=intento, importe=Decimal("400.00"), fecha=timezone.localdate(), registrado_por=self.compras,
        )
        self.assertEqual(intento.total_reembolsado, Decimal("400.00"))
        self.assertEqual(intento.saldo_reembolso, Decimal("600.00"))
        self.assertEqual(intento.compra.importe_final, Decimal("1000.00"))
```

- [ ] **Step 2: Ejecutar las pruebas y comprobar que fallan por modelos inexistentes**

Run:

```bash
python3 manage.py test compras.tests_intentos_compra.IntentoCompraModelTests --keepdb
```

Expected: `ImportError` o `AttributeError` porque `IntentoCompraDepartamental`, `ReembolsoCompraDepartamental` e `intento_vigente` aún no existen.

- [ ] **Step 3: Implementar los modelos y propiedades mínimas**

Agregar en `compras/models.py`:

```python
class IntentoCompraDepartamental(models.Model):
    ESTADO_VIGENTE = "VIGENTE"
    ESTADO_CANCELADO_SIN_PAGO = "CANCELADO_SIN_PAGO"
    ESTADO_REEMBOLSO_SOLICITADO = "REEMBOLSO_SOLICITADO"
    ESTADO_REEMBOLSADO = "REEMBOLSADO"
    ESTADO_ENTREGADO = "ENTREGADO"
    ESTADO_CHOICES = [
        (ESTADO_VIGENTE, "Vigente"),
        (ESTADO_CANCELADO_SIN_PAGO, "Cancelado sin pago"),
        (ESTADO_REEMBOLSO_SOLICITADO, "Reembolso solicitado"),
        (ESTADO_REEMBOLSADO, "Reembolsado"),
        (ESTADO_ENTREGADO, "Entregado"),
    ]
    MOTIVO_PROVEEDOR_CANCELO = "PROVEEDOR_CANCELO"
    MOTIVO_NO_ENTREGO = "NO_ENTREGO"
    MOTIVO_OTRO = "OTRO"

    item = models.ForeignKey(
        ItemCompraDepartamental, on_delete=models.PROTECT, related_name="intentos_compra"
    )
    cotizacion = models.ForeignKey(
        CotizacionCompraDepartamental, on_delete=models.PROTECT, related_name="intentos_compra"
    )
    estado = models.CharField(max_length=30, choices=ESTADO_CHOICES, default=ESTADO_VIGENTE, db_index=True)
    version = models.PositiveIntegerField(default=1)
    motivo_cancelacion = models.CharField(max_length=30, blank=True, default="")
    detalle_cancelacion = models.TextField(blank=True, default="")
    cancelado_en = models.DateTimeField(null=True, blank=True)
    cancelado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.PROTECT,
        related_name="intentos_compra_cancelados",
    )
    reembolso_solicitado = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    reembolso_solicitado_en = models.DateField(null=True, blank=True)
    evidencia_solicitud_reembolso = models.FileField(
        upload_to="compras/departamentales/reembolsos/solicitudes/%Y/%m/", null=True, blank=True
    )
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["creado_en", "pk"]
        constraints = [
            models.UniqueConstraint(
                fields=["item"], condition=models.Q(estado="VIGENTE"),
                name="comp_dept_un_intento_vigente",
            ),
        ]

    @property
    def total_reembolsado(self):
        return self.reembolsos.aggregate(total=models.Sum("importe"))["total"] or Decimal("0")

    @property
    def saldo_reembolso(self):
        return max((self.reembolso_solicitado or Decimal("0")) - self.total_reembolsado, Decimal("0"))


class ReembolsoCompraDepartamental(models.Model):
    intento = models.ForeignKey(
        IntentoCompraDepartamental, on_delete=models.PROTECT, related_name="reembolsos"
    )
    importe = models.DecimalField(max_digits=14, decimal_places=2)
    fecha = models.DateField()
    referencia = models.CharField(max_length=160, blank=True, default="")
    comprobante = models.FileField(
        upload_to="compras/departamentales/reembolsos/recibidos/%Y/%m/", null=True, blank=True
    )
    registrado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    creado_en = models.DateTimeField(default=timezone.now)
```

Agregar en `ItemCompraDepartamental`:

```python
@property
def intento_vigente(self):
    intentos = getattr(self, "intentos_compra_prefetched", None)
    if intentos is not None:
        return next((intento for intento in intentos if intento.estado == "VIGENTE"), None)
    return self.intentos_compra.filter(estado="VIGENTE").select_related(
        "cotizacion__proveedor", "linea_orden", "compra"
    ).first()
```

Cambiar las relaciones singulares por estos campos concretos:

```python
# LineaOrdenCompraDepartamental
item = models.ForeignKey(
    ItemCompraDepartamental, on_delete=models.PROTECT, related_name="lineas_orden"
)
intento = models.OneToOneField(
    IntentoCompraDepartamental, on_delete=models.PROTECT, related_name="linea_orden"
)

# CompraRealizadaDepartamental
item = models.ForeignKey(
    ItemCompraDepartamental, on_delete=models.PROTECT, related_name="compras_realizadas"
)
intento = models.OneToOneField(
    IntentoCompraDepartamental, on_delete=models.PROTECT, related_name="compra"
)

# CompromisoCompraDepartamental
item = models.ForeignKey(
    ItemCompraDepartamental, on_delete=models.CASCADE, related_name="compromisos"
)
intento = models.OneToOneField(
    IntentoCompraDepartamental, null=True, blank=True,
    on_delete=models.PROTECT, related_name="compromiso",
)
```

El `null` del compromiso es intencional: una reserva autorizada existe antes de generar su intento/orden. Agregar además una restricción condicional que permita como máximo un compromiso activo sin intento por artículo:

```python
models.UniqueConstraint(
    fields=["item"],
    condition=models.Q(activo=True, intento__isnull=True),
    name="comp_dept_una_reserva_preorden_activa",
)
```

- [ ] **Step 4: Crear la migración con backfill antes de restricciones no nulas**

Generar el esqueleto:

```bash
python3 manage.py makemigrations compras --name intentos_compra_reembolsos
```

Editar `0016_intentos_compra_reembolsos.py` para ordenar operaciones así:

1. Crear `IntentoCompraDepartamental` y `ReembolsoCompraDepartamental`.
2. Agregar `intento` nullable a línea, compra y compromiso.
3. Alterar las relaciones `item` a `ForeignKey`.
4. Ejecutar `RunPython(crear_intentos_historicos, migrations.RunPython.noop)`.
5. Convertir `LineaOrdenCompraDepartamental.intento` y `CompraRealizadaDepartamental.intento` a no nulos. `CompromisoCompraDepartamental.intento` permanece nullable para reservas previas a la orden.
6. Agregar la restricción de un intento vigente.

El backfill debe agrupar por línea existente y clasificar conservadoramente:

```python
def crear_intentos_historicos(apps, schema_editor):
    Intento = apps.get_model("compras", "IntentoCompraDepartamental")
    Linea = apps.get_model("compras", "LineaOrdenCompraDepartamental")
    Compra = apps.get_model("compras", "CompraRealizadaDepartamental")
    Compromiso = apps.get_model("compras", "CompromisoCompraDepartamental")
    db = schema_editor.connection.alias
    for linea in Linea.objects.using(db).select_related("item").order_by("pk"):
        estado = "ENTREGADO" if linea.item.estado in ("PENDIENTE_CONFIRMACION", "RECIBIDO_CONFORME") else "VIGENTE"
        intento = Intento.objects.using(db).create(
            item_id=linea.item_id, cotizacion_id=linea.cotizacion_id, estado=estado
        )
        Linea.objects.using(db).filter(pk=linea.pk).update(intento_id=intento.pk)
        Compra.objects.using(db).filter(item_id=linea.item_id).update(intento_id=intento.pk)
        Compromiso.objects.using(db).filter(item_id=linea.item_id).update(intento_id=intento.pk)
```

Antes de volver no nulo `CompraRealizadaDepartamental.intento`, la migración debe abortar con un error legible si existe una compra sin línea de orden; no debe inventar proveedor u orden.

- [ ] **Step 5: Verificar migración y contrato**

Run:

```bash
python3 manage.py migrate compras 0015
python3 manage.py migrate compras 0016
python3 manage.py migrate --check
python3 manage.py test compras.tests_intentos_compra.IntentoCompraModelTests --keepdb
```

Expected: migración completa, cero migraciones pendientes y pruebas `OK`.

- [ ] **Step 6: Confirmar el contrato de datos**

```bash
git add compras/models.py compras/migrations/0016_intentos_compra_reembolsos.py compras/tests_intentos_compra.py
git commit -m "feat(compras): modelar intentos y reembolsos departamentales"
```

### Task 2: Hacer que orden, compra, compromiso y recepción operen por intento

**Files:**
- Modify: `compras/services_departamentales.py:60-223`
- Modify: `compras/services_edicion_compra.py:18-214`
- Modify: `compras/models.py:519-538`
- Modify: `compras/tests_departamentales.py`
- Modify: `compras/tests_edicion_compra.py`
- Modify: `compras/tests_avisos_compra.py`

- [ ] **Step 1: Escribir pruebas fallidas de segundo intento y regresión**

Añadir:

```python
def test_generar_orden_crea_intento_y_compromiso_vinculados(self):
    orden = generar_ordenes_departamentales([self.item], actor=self.compras)[0]
    linea = orden.lineas.get()
    self.assertEqual(linea.intento.item, self.item)
    self.assertEqual(linea.intento.cotizacion, self.cotizacion)
    self.assertEqual(linea.intento.compromiso.item, self.item)

def test_compra_se_registra_en_intento_vigente_y_aviso_conserva_compra(self):
    intento = self.ordenar()
    compra = registrar_compra_realizada(self.item, **self.datos_compra(), actor=self.compras)
    self.assertEqual(compra.intento, intento)
    self.assertEqual(compra.avisos.count(), 2)

def test_recepcion_total_marca_intento_entregado_y_area_pendiente(self):
    intento = self.comprar()
    RecepcionItemDepartamental.objects.create(
        linea_orden=intento.linea_orden, cantidad_recibida=self.item.cantidad,
        registrado_por=self.compras,
    )
    intento.refresh_from_db()
    self.item.refresh_from_db()
    self.assertEqual(intento.estado, IntentoCompraDepartamental.ESTADO_ENTREGADO)
    self.assertEqual(self.item.estado, ItemCompraDepartamental.ESTADO_PENDIENTE_CONFIRMACION)
    self.assertEqual(self.item.siguiente_responsable, ItemCompraDepartamental.RESPONSABLE_AREA)
```

- [ ] **Step 2: Ejecutar pruebas y comprobar que fallan en asociaciones antiguas**

Run:

```bash
python3 manage.py test compras.tests_departamentales compras.tests_edicion_compra compras.tests_avisos_compra --keepdb
```

Expected: fallos al no crear o consultar `intento` en orden, compra y compromiso.

- [ ] **Step 3: Adaptar creación y consulta del intento vigente**

En `generar_ordenes_departamentales`:

```python
intento = IntentoCompraDepartamental.objects.create(item=item, cotizacion=cotizacion)
linea = LineaOrdenCompraDepartamental.objects.create(
    orden=orden, intento=intento, item=item, cotizacion=cotizacion,
    cantidad=item.cantidad, costo_unitario=cotizacion.costo_unitario,
    total=cotizacion.total_adquisicion,
)
CompromisoCompraDepartamental.objects.filter(item=item, activo=True, intento__isnull=True).update(
    intento=intento, formalizado_en=timezone.now()
)
```

Reemplazar búsquedas singulares `item.linea_orden` por `item.intento_vigente.linea_orden`. `sincronizar_linea_orden` debe devolver la línea del intento vigente o `None`, nunca la primera línea histórica.

En `registrar_compra_realizada`, bloquear solo si el intento vigente ya tiene compra o recepción, obtener la línea del intento, y crear:

```python
compra = CompraRealizadaDepartamental(
    intento=intento, item=item, cotizacion=cotizacion, fecha_compra=fecha_compra,
    importe_final=importe_final, numero_pedido=numero_pedido,
    comprobante=comprobante, registrado_por=actor,
)
```

Actualizar únicamente `intento.compromiso`. `tiene_compra_o_recepcion(item)` debe significar evidencia en el intento vigente; añadir otra función `tiene_recepcion_historica(item)` para las decisiones que realmente deban considerar cualquier recepción. `corregir_compra_realizada` actualizará solo el compromiso de esa compra y, si el intento está en reembolso, rechazará un nuevo importe inferior al reembolso ya solicitado.

- [ ] **Step 4: Marcar el intento entregado al completar la cantidad**

En `RecepcionItemDepartamental.save`, bloquear intentos no vigentes y, al cubrir la cantidad, guardar el intento como `ENTREGADO` antes de dejar el artículo en `PENDIENTE_CONFIRMACION`. Mantener la confirmación del área separada.

- [ ] **Step 5: Ejecutar regresiones completas del flujo existente**

Run:

```bash
python3 manage.py test compras.tests_departamentales compras.tests_edicion_compra compras.tests_avisos_compra --keepdb
```

Expected: todas las pruebas `OK`, incluidos idempotencia de avisos, corrección de compra y bloqueo por recepción.

- [ ] **Step 6: Confirmar la adaptación del flujo base**

```bash
git add compras/models.py compras/services_departamentales.py compras/services_edicion_compra.py compras/tests_departamentales.py compras/tests_edicion_compra.py compras/tests_avisos_compra.py
git commit -m "refactor(compras): operar órdenes y recepciones por intento"
```

### Task 3: Implementar cancelación y seguimiento de reembolsos

**Files:**
- Create: `compras/services_intentos_compra.py`
- Create: `compras/forms_intentos_compra.py`
- Modify: `compras/tests_intentos_compra.py`

- [ ] **Step 1: Escribir pruebas fallidas del servicio de cancelación**

```python
class CancelacionIntentoTests(CompraDepartamentalIntentoBase):
    def test_cancelar_orden_sin_pago_libera_compromiso_y_reabre_cotizacion(self):
        intento = self.ordenar()
        cancelar_intento(
            intento, motivo="NO_ENTREGO", detalle="Proveedor confirmó que no surtirá",
            actor=self.compras, version=intento.version,
        )
        intento.refresh_from_db()
        self.item.refresh_from_db()
        self.assertEqual(intento.estado, "CANCELADO_SIN_PAGO")
        self.assertFalse(intento.compromiso.activo)
        self.assertEqual(self.item.estado, "POR_COTIZAR")
        self.assertFalse(self.cotizacion.seleccionada)

    def test_cancelar_compra_pagada_deja_exposicion_y_solicita_reembolso(self):
        intento = self.comprar(importe=Decimal("1000.00"))
        cancelar_intento(
            intento, motivo="PROVEEDOR_CANCELO", detalle="Canceló después del cargo",
            actor=self.compras, version=intento.version,
            fecha_solicitud_reembolso=timezone.localdate(),
            importe_reembolso=Decimal("1000.00"),
        )
        intento.refresh_from_db()
        self.assertEqual(intento.estado, "REEMBOLSO_SOLICITADO")
        self.assertTrue(intento.compromiso.activo)
        self.assertEqual(intento.saldo_reembolso, Decimal("1000.00"))

    def test_no_cancela_si_existe_recepcion_positiva(self):
        intento = self.comprar()
        self.recibir(intento, Decimal("1"))
        with self.assertRaisesMessage(ValidationError, "recepción"):
            cancelar_intento(
                intento, motivo="NO_ENTREGO", detalle="No llegó el resto",
                actor=self.compras, version=intento.version,
            )

    def test_cancelar_articulo_definitivamente_exige_no_tener_reembolso_pendiente(self):
        intento = self.intento_con_reembolso_pendiente(Decimal("1000.00"))
        with self.assertRaisesMessage(ValidationError, "reembolso pendiente"):
            cancelar_articulo_definitivamente(
                self.item, motivo="El área ya no lo necesita", actor=self.compras,
            )
```

- [ ] **Step 2: Escribir pruebas fallidas de reembolso parcial, total y concurrencia**

```python
def test_reembolso_parcial_conserva_saldo_y_compromiso(self):
    intento = self.intento_con_reembolso_pendiente(Decimal("1000.00"))
    registrar_reembolso(
        intento, importe=Decimal("400.00"), fecha=timezone.localdate(),
        referencia="DEV-1", comprobante=None, actor=self.compras,
        version=intento.version,
    )
    intento.refresh_from_db()
    self.assertEqual(intento.estado, "REEMBOLSO_SOLICITADO")
    self.assertEqual(intento.saldo_reembolso, Decimal("600.00"))
    self.assertTrue(intento.compromiso.activo)

def test_reembolso_total_cierra_y_libera_compromiso(self):
    intento = self.intento_con_reembolso_pendiente(Decimal("1000.00"))
    registrar_reembolso(
        intento, importe=Decimal("1000.00"), fecha=timezone.localdate(),
        referencia="DEV-TOTAL", comprobante=None, actor=self.compras,
        version=intento.version,
    )
    intento.refresh_from_db()
    self.assertEqual(intento.estado, "REEMBOLSADO")
    self.assertFalse(intento.compromiso.activo)

def test_version_antigua_no_duplica_reembolso(self):
    intento = self.intento_con_reembolso_pendiente(Decimal("1000.00"))
    version = intento.version
    self.registrar_reembolso(intento, Decimal("400.00"), version=version)
    with self.assertRaisesMessage(ValidationError, "actualizó"):
        self.registrar_reembolso(intento, Decimal("400.00"), version=version)
    self.assertEqual(intento.reembolsos.count(), 1)
```

- [ ] **Step 3: Ejecutar pruebas y comprobar que fallan por servicios inexistentes**

Run:

```bash
python3 manage.py test compras.tests_intentos_compra.CancelacionIntentoTests compras.tests_intentos_compra.ReembolsoIntentoTests --keepdb
```

Expected: fallos de importación para `cancelar_intento` y `registrar_reembolso`.

- [ ] **Step 4: Implementar servicios transaccionales**

Crear `compras/services_intentos_compra.py` con:

```python
@transaction.atomic
def cancelar_intento(intento, *, motivo, detalle, actor, version,
                     fecha_solicitud_reembolso=None, importe_reembolso=None,
                     evidencia=None):
    intento = (IntentoCompraDepartamental.objects.select_for_update()
               .select_related("item__solicitud", "cotizacion", "compromiso")
               .get(pk=intento.pk))
    if intento.version != version:
        raise ValidationError("Otra persona actualizó este intento. Recarga y revisa los cambios.")
    if intento.estado != IntentoCompraDepartamental.ESTADO_VIGENTE:
        raise ValidationError("Este intento ya no está vigente.")
    if RecepcionItemDepartamental.objects.filter(
        linea_orden__intento=intento, cantidad_recibida__gt=0
    ).exists():
        raise ValidationError("No puedes cancelar un intento con recepción positiva.")
    if motivo not in {"PROVEEDOR_CANCELO", "NO_ENTREGO", "OTRO"} or not detalle.strip():
        raise ValidationError("Selecciona un motivo y explica lo ocurrido.")

    compra = getattr(intento, "compra", None)
    if compra:
        if not fecha_solicitud_reembolso or not importe_reembolso:
            raise ValidationError("Registra fecha e importe del reembolso solicitado.")
        if importe_reembolso <= 0 or importe_reembolso > compra.importe_final:
            raise ValidationError("El reembolso solicitado debe estar entre cero y el importe pagado.")
        intento.estado = IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO
        intento.reembolso_solicitado = importe_reembolso
        intento.reembolso_solicitado_en = fecha_solicitud_reembolso
        intento.evidencia_solicitud_reembolso = evidencia
    else:
        intento.estado = IntentoCompraDepartamental.ESTADO_CANCELADO_SIN_PAGO
        intento.compromiso.activo = False
        intento.compromiso.liberado_en = timezone.now()
        intento.compromiso.save(update_fields=["activo", "liberado_en"])

    intento.motivo_cancelacion = motivo
    intento.detalle_cancelacion = detalle.strip()
    intento.cancelado_en = timezone.now()
    intento.cancelado_por = actor
    intento.version += 1
    intento.save()
    intento.cotizacion.seleccionada = False
    intento.cotizacion.save(update_fields=["seleccionada"])
    item = intento.item
    item.estado = ItemCompraDepartamental.ESTADO_POR_COTIZAR
    item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_COMPRAS
    item.comentario_reciente = detalle.strip()
    item.save(update_fields=["estado", "siguiente_responsable", "comentario_reciente", "actualizado_en"])
    item.solicitud.actualizar_estado_desde_items()
    EventoCompraDepartamental.objects.create(
        solicitud=item.solicitud, item=item, actor=actor,
        tipo="INTENTO_CANCELADO", detalle=detalle.strip(),
    )
    return intento
```

Implementar `registrar_reembolso` con `select_for_update`, validación de versión/estado/fecha/importe, creación append-only de `ReembolsoCompraDepartamental`, incremento de versión, evento `REEMBOLSO_RECIBIDO` y liberación del compromiso únicamente cuando `saldo_reembolso == 0`.

Implementar `cancelar_articulo_definitivamente(item, *, motivo, actor)` con bloqueo del artículo. Debe exigir motivo, ausencia de recepciones positivas, ausencia de intento vigente y ausencia de cualquier saldo de reembolso. Después cambia el artículo a `CANCELADO`, asigna `RESPONSABLE_NADIE`, recalcula la solicitud y crea `ARTICULO_CANCELADO`.

- [ ] **Step 5: Implementar formularios dependientes del estado persistido**

Crear `CancelarIntentoCompraForm` con `motivo`, `detalle`, `version` y, solo si el intento tiene compra, `fecha_solicitud_reembolso`, `importe_reembolso`, `evidencia`. Crear `RegistrarReembolsoCompraForm` con `fecha`, `importe`, `referencia`, `comprobante`, `version`. Reutilizar `validar_comprobante` y validar fecha no futura.

- [ ] **Step 6: Ejecutar pruebas del dominio y confirmar**

Run:

```bash
python3 manage.py test compras.tests_intentos_compra --keepdb
```

Expected: todas las pruebas `OK`.

```bash
git add compras/services_intentos_compra.py compras/forms_intentos_compra.py compras/tests_intentos_compra.py
git commit -m "feat(compras): cancelar intentos y registrar reembolsos"
```

### Task 4: Exponer acciones protegidas con contexto estable

**Files:**
- Create: `compras/views_intentos_compra.py`
- Modify: `compras/urls.py:1-25`
- Modify: `compras/tests_intentos_compra.py`
- Modify: `docs/ux/action-context-coverage.md`

- [ ] **Step 1: Escribir pruebas fallidas de permisos y contrato progresivo**

```python
class IntentoCompraViewTests(CompraDepartamentalIntentoBase):
    def test_area_no_puede_cancelar_intento(self):
        self.client.force_login(self.solicitante)
        response = self.client.post(self.url_cancelar(), self.datos_cancelacion())
        self.assertEqual(response.status_code, 403)

    def test_compras_cancela_y_json_regresa_al_mismo_articulo(self):
        self.client.force_login(self.compras)
        response = self.client.post(
            self.url_cancelar(), self.datos_cancelacion(),
            HTTP_X_REQUESTED_WITH="XMLHttpRequest", HTTP_ACCEPT="application/json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertTrue(response.json()["redirect_url"].endswith(f"#item-{self.item.pk}"))

    def test_error_conserva_formulario_y_no_duplica_accion(self):
        self.client.force_login(self.compras)
        datos = self.datos_reembolso(importe="999999.00")
        response = self.client.post(self.url_reembolsar(), datos)
        self.assertEqual(response.status_code, 400)
        self.assertContains(response, "importe", status_code=400)
        self.assertEqual(self.intento.reembolsos.count(), 0)
```

- [ ] **Step 2: Ejecutar pruebas y comprobar rutas inexistentes**

Run:

```bash
python3 manage.py test compras.tests_intentos_compra.IntentoCompraViewTests --keepdb
```

Expected: `NoReverseMatch` para las rutas nuevas.

- [ ] **Step 3: Crear vistas y rutas**

Añadir en `compras/urls.py`:

```python
path(
    "departamentales/intentos/<int:pk>/cancelar/",
    views_intentos_compra.departamental_intento_cancelar,
    name="departamental_intento_cancelar",
),
path(
    "departamentales/intentos/<int:pk>/reembolsar/",
    views_intentos_compra.departamental_reembolso_registrar,
    name="departamental_reembolso_registrar",
),
path(
    "departamentales/items/<int:item_pk>/cancelar/",
    views_intentos_compra.departamental_articulo_cancelar,
    name="departamental_articulo_cancelar",
),
```

Las vistas deben usar `login_required`, `require_http_methods(["GET", "POST"])`, `puede_gestionar_compras_departamentales`, `_mostrar_formulario`, `_respuesta_accion` y `_destino`. El éxito devuelve toast y `reload=True`; un conflicto de versión devuelve `409`; errores de formulario devuelven `400` y conservan campos. El formulario de cancelación se renderiza dentro del panel del intento y lleva `data-confirm-message="Se cancelará este intento con el proveedor y se conservará todo su historial. ¿Continuar?"`, activando el modal global accesible de `base.html`. La cancelación definitiva usa su ruta propia, exige el motivo en POST y el mensaje `Este artículo dejará de buscarse y la solicitud puede cerrarse. ¿Continuar?`.

- [ ] **Step 4: Registrar cobertura de contexto**

Agregar a `docs/ux/action-context-coverage.md` dos renglones:

```markdown
| Compras departamentales | Proveedor canceló / no entregó | Cubierto | `data-async-action`, modal accesible, toast y retorno a `#item-<id>` |
| Compras departamentales | Confirmar reembolso recibido | Cubierto | `data-async-action`, modal accesible, toast y retorno a `#item-<id>` |
| Compras departamentales | Cancelar definitivamente el artículo | Cubierto | Motivo obligatorio, modal accesible, toast y retorno a `#item-<id>` |
```

- [ ] **Step 5: Ejecutar pruebas y confirmar**

```bash
python3 manage.py test compras.tests_intentos_compra.IntentoCompraViewTests --keepdb
git add compras/views_intentos_compra.py compras/urls.py compras/tests_intentos_compra.py docs/ux/action-context-coverage.md
git commit -m "feat(compras): exponer cancelación y reembolso auditables"
```

Expected: pruebas `OK` y commit sin archivos ajenos.

### Task 5: Mostrar intentos, reembolsos y responsable real de la entrega

**Files:**
- Modify: `compras/views_departamentales.py:292-361`
- Modify: `compras/templates/compras/departamentales/detalle.html:1-116`
- Modify: `static/css/compras_departamentales.css`
- Modify: `compras/tests_intentos_compra.py`

- [ ] **Step 1: Escribir pruebas fallidas de contenido visible**

```python
class IntentoCompraTemplateTests(CompraDepartamentalIntentoBase):
    def test_cantidad_total_explica_que_el_area_debe_confirmar(self):
        intento = self.comprar_y_recibir_total()
        self.client.force_login(self.compras)
        response = self.client.get(self.url_detalle())
        self.assertContains(response, "Entregado por Compras")
        self.assertContains(response, "Pendiente de confirmación del área")
        self.assertContains(response, self.solicitud.area.nombre)
        self.assertNotContains(response, "Registrar entrega")

    def test_compra_no_entregada_ofrece_cancelar_no_capturar_cero(self):
        intento = self.comprar()
        self.client.force_login(self.compras)
        response = self.client.get(self.url_detalle())
        self.assertContains(response, "Proveedor canceló / no entregó")
        self.assertContains(response, reverse("compras:departamental_intento_cancelar", args=[intento.pk]))

    def test_historial_muestra_reembolso_y_reemplazo_sin_borrar_primero(self):
        cancelado = self.intento_con_reembolso_pendiente(Decimal("1000.00"))
        reemplazo = self.crear_intento(estado="VIGENTE", proveedor=self.otro_proveedor)
        self.client.force_login(self.compras)
        response = self.client.get(self.url_detalle())
        self.assertContains(response, cancelado.cotizacion.proveedor.nombre)
        self.assertContains(response, "Reembolso pendiente")
        self.assertContains(response, reemplazo.cotizacion.proveedor.nombre)
```

- [ ] **Step 2: Ejecutar pruebas y comprobar textos/acciones ausentes**

Run:

```bash
python3 manage.py test compras.tests_intentos_compra.IntentoCompraTemplateTests --keepdb
```

Expected: fallos `assertContains` para estados y acciones nuevas.

- [ ] **Step 3: Precargar intentos completos sin N+1**

En `departamental_detalle`, usar `Prefetch` ordenado:

```python
intentos = (IntentoCompraDepartamental.objects
    .select_related("cotizacion__proveedor", "linea_orden__orden", "compra", "compromiso", "cancelado_por")
    .prefetch_related("reembolsos__registrado_por", "compra__avisos", "compra__historial__actor")
    .order_by("creado_en", "pk"))
```

Prefetch a `items__intentos_compra` con `to_attr="intentos_compra_prefetched"`. Calcular por artículo `intento_vigente`, `reembolsos_pendientes`, `puede_cancelar_intento`, `puede_registrar_reembolso` y la etiqueta del responsable de confirmación.

- [ ] **Step 4: Reemplazar el bloque singular por línea de tiempo**

En `detalle.html`, iterar `item.intentos_compra_prefetched`. Mostrar una tarjeta por intento con proveedor, orden, compra, estado, motivo, saldo y acciones. Mantener enlaces de comprobante/corrección/avisos dentro del intento correspondiente. Para `PENDIENTE_CONFIRMACION`, renderizar literalmente:

```html
<div class="cd-delivery-state cd-delivery-state--waiting">
  <strong>Entregado por Compras</strong>
  <span>Pendiente de confirmación del área {{ solicitud.area.nombre }}.</span>
</div>
```

El formulario de entrega conserva `min="0.001"`; junto a él se muestra la acción separada de cancelación. No aceptar ni sugerir `0` como recepción. Cuando no exista intento vigente, recepción positiva ni saldo de reembolso, mostrar en un panel secundario `Cancelar definitivamente el artículo` con motivo obligatorio.

- [ ] **Step 5: Añadir estilos accesibles y versión del activo**

Crear estilos para `.cd-attempt-timeline`, `.cd-attempt-card`, `.cd-refund-state` y `.cd-delivery-state`, sin depender solo del color. Mantener contraste AA, foco visible y `prefers-reduced-motion`. Incrementar el querystring de `compras_departamentales.css` en `detalle.html`. El módulo no tiene service worker propio, por lo que no se agrega un bump de `CACHE_NAME`.

- [ ] **Step 6: Ejecutar pruebas de plantilla y confirmar**

```bash
python3 manage.py test compras.tests_intentos_compra.IntentoCompraTemplateTests --keepdb
git add compras/views_departamentales.py compras/templates/compras/departamentales/detalle.html static/css/compras_departamentales.css compras/tests_intentos_compra.py
git commit -m "feat(compras): aclarar entregas e historial de proveedores"
```

Expected: pruebas `OK`; la entrega completa señala al área y no se presenta como simple pendiente de Compras.

### Task 6: Separar compromiso vigente, reembolso pendiente y reembolsado

**Files:**
- Modify: `compras/resumen_departamentales.py:18-173`
- Modify: `compras/templates/compras/departamentales/bandeja.html`
- Modify: `compras/tests_resumen_departamentales.py`
- Modify: `compras/tests_intentos_compra.py`

- [ ] **Step 1: Escribir pruebas fallidas de presupuesto de reemplazo**

```python
def test_reembolso_pendiente_sigue_consumiendo_disponible_del_reemplazo(self):
    anterior = self.intento_con_reembolso_pendiente(Decimal("1000.00"))
    evaluacion = evaluar_presupuesto_item(self.item, Decimal("500.00"))
    self.assertEqual(evaluacion.compromisos_previos, Decimal("1000.00"))
    self.assertEqual(evaluacion.disponible_despues, self.presupuesto - self.real - Decimal("1500.00"))
```

Cambiar `evaluar_presupuesto_item` para excluir solo el compromiso que se está reevaluando, no todos los compromisos del mismo artículo. Añadir parámetro opcional `compromiso_excluido=None` y usar `.exclude(pk=compromiso_excluido.pk)`.

- [ ] **Step 2: Escribir pruebas fallidas del resumen y XLSX**

```python
def test_resumen_separa_comprometido_reembolso_pendiente_y_reembolsado(self):
    self.crear_compromiso_vigente(Decimal("500.00"))
    self.crear_reembolso_pendiente(Decimal("1000.00"), recibido=Decimal("250.00"))
    contexto = construir_resumen_departamental(self.request())
    self.assertEqual(contexto["resumen"]["comprometido"], Decimal("500.00"))
    self.assertEqual(contexto["resumen"]["reembolso_pendiente"], Decimal("750.00"))
    self.assertEqual(contexto["resumen"]["reembolsado"], Decimal("250.00"))

def test_xlsx_contiene_columnas_separadas_sin_formulas(self):
    response = exportar_resumen_departamental(construir_resumen_departamental(self.request()))
    workbook = load_workbook(BytesIO(response.content), data_only=False)
    headers = [cell.value for cell in workbook["Resumen"][8]]
    self.assertIn("Reembolso pendiente", headers)
    self.assertIn("Reembolsado", headers)
    self.assertFalse(any(
        isinstance(cell.value, str) and cell.value.startswith("=")
        for sheet in workbook for row in sheet.iter_rows() for cell in row
    ))
```

- [ ] **Step 3: Ejecutar pruebas y comprobar columnas ausentes**

Run:

```bash
python3 manage.py test compras.tests_resumen_departamentales compras.tests_intentos_compra.PresupuestoReembolsoTests --keepdb
```

Expected: fallos por claves y encabezados nuevos ausentes.

- [ ] **Step 4: Implementar agregados separados**

Precargar compromisos e intentos. Para cada artículo calcular:

```python
item.resumen_comprometido = sum(
    compromiso.monto for compromiso in item.compromisos.all()
    if compromiso.activo and compromiso.formalizado_en
    and compromiso.intento.estado == IntentoCompraDepartamental.ESTADO_VIGENTE
)
item.resumen_reembolso_pendiente = sum(
    intento.saldo_reembolso for intento in item.intentos_compra_prefetched
    if intento.estado == IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO
)
item.resumen_reembolsado = sum(
    intento.total_reembolsado for intento in item.intentos_compra_prefetched
)
```

Añadir ambas columnas al KPI, desglose por área, detalle XLSX y leyenda `ETAPAS`. No sumar reembolso pendiente dentro de `comprometido`; mostrarlo separado, aunque siga participando en la evaluación presupuestal.

- [ ] **Step 5: Ejecutar pruebas y confirmar**

```bash
python3 manage.py test compras.tests_resumen_departamentales compras.tests_intentos_compra.PresupuestoReembolsoTests --keepdb
git add compras/services_departamentales.py compras/resumen_departamentales.py compras/templates/compras/departamentales/bandeja.html compras/tests_resumen_departamentales.py compras/tests_intentos_compra.py
git commit -m "feat(compras): separar reembolsos en presupuesto y resumen"
```

Expected: pruebas `OK`; XLSX y HTML muestran cifras conciliables sin doble conteo.

### Task 7: Validar el flujo completo, revisar consumidores y preparar entrega

**Files:**
- Modify only if a failing consumer requires it: `compras/tasks.py`, `compras/services_avisos_compra.py`, `compras/templates/compras/emails/compra_realizada.html`, `compras/templates/compras/emails/compra_realizada.txt`
- Review: all files changed in Tasks 1-6

- [ ] **Step 1: Buscar consumidores singulares restantes**

Run:

```bash
rg -n "\.compra_realizada\b|\.linea_orden\b|related_name=[\"']compra_realizada|related_name=[\"']linea_orden" compras reportes api
```

Expected: ningún consumidor trata la relación histórica como singular; los usos válidos atraviesan `intento.compra` o `intento.linea_orden`.

- [ ] **Step 2: Ejecutar comprobaciones de migración y Django**

Run:

```bash
python3 manage.py makemigrations --check
python3 manage.py migrate --check
python3 manage.py showmigrations compras
python3 manage.py check
```

Expected: cero cambios de modelo sin migración, cero migraciones pendientes, `compras.0016` aplicada y `System check identified no issues`.

- [ ] **Step 3: Ejecutar todas las pruebas afectadas**

Run:

```bash
python3 manage.py test \
  compras.tests_intentos_compra \
  compras.tests_departamentales \
  compras.tests_edicion_compra \
  compras.tests_avisos_compra \
  compras.tests_resumen_departamentales \
  --keepdb
```

Expected: suite completa `OK`.

- [ ] **Step 4: Validar en navegador local con datos de prueba**

Crear únicamente fixtures locales. Verificar como Compras:

1. Orden sin pago → `Proveedor canceló / no entregó` → compromiso liberado → nueva cotización.
2. Compra pagada → solicitud de reembolso → reemplazo con otro proveedor visible junto al antecedente.
3. Reembolso parcial y total → saldo correcto y botón idempotente.
4. Recepción total → texto `Entregado por Compras · pendiente de confirmación del área`.
5. Como responsable del área → `Recibido conforme` → artículo y solicitud completados.
6. Consola sin errores, solicitudes XHR exitosas, foco restaurado y fragmento `#item-<id>` preservado.

- [ ] **Step 5: Revisar diff completo y confirmar correcciones finales**

Run:

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff origin/main..HEAD -- compras docs/ux/action-context-coverage.md static/css/compras_departamentales.css
git log --oneline --decorate -10
git worktree list
```

Expected: solo archivos de esta tarea, sin capturas, logs, `.DS_Store`, outputs ni cambios de otros módulos.

Si la revisión exige una corrección, ejecutar su prueba específica, agregar únicamente los archivos de esta funcionalidad que hayan cambiado y confirmar quirúrgicamente:

```bash
git add compras/models.py compras/services_intentos_compra.py compras/services_departamentales.py compras/services_edicion_compra.py compras/forms_intentos_compra.py compras/views_intentos_compra.py compras/views_departamentales.py compras/urls.py compras/resumen_departamentales.py compras/templates/compras/departamentales/detalle.html compras/templates/compras/departamentales/bandeja.html static/css/compras_departamentales.css compras/tests_intentos_compra.py compras/tests_departamentales.py compras/tests_edicion_compra.py compras/tests_avisos_compra.py compras/tests_resumen_departamentales.py docs/ux/action-context-coverage.md
git commit -m "fix(compras): corregir validación de cancelación departamental"
```

- [ ] **Step 6: Abrir PR en borrador y esperar CI**

La descripción debe incluir resumen funcional, migración `compras.0016`, archivos principales, pruebas ejecutadas, validación en navegador y ausencia de escrituras productivas. Verificar que CI requerido termine verde antes del merge.

- [ ] **Step 7: Mergear, desplegar y verificar producción**

Después del merge:

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && bash scripts/deploy_web_safe.sh'
```

No ejecutar `git pull` manual antes. Confirmar en la salida que se aplicó `compras.0016`, `collectstatic` terminó y los procesos correspondientes cargaron el nuevo código.

Validar autenticado en `https://erp.pollyanasdolce.com`:

- una compra controlada o preparada específicamente para la verificación, sin cancelar ni modificar compras reales no autorizadas;
- visibilidad de acciones según permisos;
- estado claro de entrega pendiente del área;
- totales y XLSX sin doble conteo;
- consola, XHR y archivos estáticos actuales.

No ejecutar una cancelación o reembolso real en producción sin autorización explícita para ese registro. Si no existe un caso seguro, validar lectura/UI y dejar documentada la prueba transaccional local como límite.

- [ ] **Step 8: Cerrar el worktree por el ciclo oficial**

Una vez mergeado, desplegado y validado:

```bash
bash scripts/task_workspace_audit.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1
bash scripts/task_workspace_close.sh \
  --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 \
  --task compras_departamentales_cancelacion_reembolso \
  --state merged
git -C /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 worktree prune --dry-run
```

Expected: tarea cerrada, worktree y ramas exactas eliminados, `origin` podado y checkout raíz limpio/sincronizado.
