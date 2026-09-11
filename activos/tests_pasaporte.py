"""Matriz de acceso y contenido del pasaporte digital de activos."""

from datetime import timedelta
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.test import RequestFactory, TestCase
from django.utils import timezone

from core.models import Sucursal, UserModuleAccess, UserProfile
from fallas.models import CategoriaFalla, ReporteFalla
from maestros.models import Proveedor

from .models import Activo, OrdenMantenimiento, PlanMantenimiento
from .services_pasaporte import (
    activos_autorizados,
    construir_pasaporte,
    puede_reportar_activo,
    svg_qr_activo,
    url_qr_activo,
)

User = get_user_model()


class PasaporteAlcanceTests(TestCase):
    def setUp(self):
        self.payan = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.leyva = Sucursal.objects.create(codigo="LEYVA", nombre="Leyva")

        self.activo_payan = Activo.objects.create(nombre="Refrigerador Payán", sucursal=self.payan)
        self.activo_leyva = Activo.objects.create(nombre="Refrigerador Leyva", sucursal=self.leyva)
        self.activo_baja = Activo.objects.create(
            nombre="Horno dado de baja", sucursal=self.payan, activo=False
        )
        self.activo_sin_sucursal = Activo.objects.create(nombre="Equipo en tránsito")

        self.operativa = User.objects.create_user(username="encargada.payan")
        UserProfile.objects.create(user=self.operativa, sucursal=self.payan)

        self.mantenimiento = User.objects.create_user(username="tecnico.mantenimiento")
        self.mantenimiento.groups.add(Group.objects.create(name="mantenimiento"))

        self.dg = User.objects.create_superuser(username="dg", password="x")

        self.sin_acceso = User.objects.create_user(username="sin.acceso")

    def test_anonimo_no_ve_ningun_activo(self):
        self.assertEqual(activos_autorizados(AnonymousUser()).count(), 0)

    def test_usuario_sin_sucursal_ni_mantenimiento_no_ve_nada(self):
        self.assertEqual(activos_autorizados(self.sin_acceso).count(), 0)

    def test_operacion_solo_ve_activos_vigentes_de_su_sucursal(self):
        visibles = set(activos_autorizados(self.operativa).values_list("pk", flat=True))
        self.assertEqual(visibles, {self.activo_payan.pk})

    def test_operacion_no_alcanza_activo_de_otra_sucursal(self):
        self.assertNotIn(
            self.activo_leyva.pk, activos_autorizados(self.operativa).values_list("pk", flat=True)
        )

    def test_operacion_no_alcanza_activo_sin_sucursal(self):
        self.assertNotIn(
            self.activo_sin_sucursal.pk,
            activos_autorizados(self.operativa).values_list("pk", flat=True),
        )

    def test_mantenimiento_global_alcanza_todo_incluido_lo_inactivo(self):
        visibles = set(activos_autorizados(self.mantenimiento).values_list("pk", flat=True))
        self.assertEqual(
            visibles,
            {
                self.activo_payan.pk,
                self.activo_leyva.pk,
                self.activo_baja.pk,
                self.activo_sin_sucursal.pk,
            },
        )

    def test_dg_alcanza_todo(self):
        self.assertEqual(activos_autorizados(self.dg).count(), 4)

    def test_mantenimiento_de_una_sola_sucursal_no_cruza_a_otra(self):
        jefa = User.objects.create_user(username="jefa.payan")
        UserProfile.objects.create(user=jefa, sucursal=self.payan)
        UserModuleAccess.objects.create(user=jefa, module="activos", access="view")

        visibles = set(activos_autorizados(jefa).values_list("pk", flat=True))
        self.assertIn(self.activo_payan.pk, visibles)
        self.assertIn(self.activo_baja.pk, visibles)
        self.assertNotIn(self.activo_leyva.pk, visibles)
        self.assertNotIn(self.activo_sin_sucursal.pk, visibles)

    def test_reportar_exige_sucursal_operativa_propia_y_activo_vigente(self):
        self.assertTrue(puede_reportar_activo(self.operativa, self.activo_payan))
        self.assertFalse(puede_reportar_activo(self.operativa, self.activo_leyva))
        self.assertFalse(puede_reportar_activo(self.operativa, self.activo_baja))
        self.assertFalse(puede_reportar_activo(self.operativa, self.activo_sin_sucursal))
        self.assertFalse(puede_reportar_activo(AnonymousUser(), self.activo_payan))

    def test_dg_sin_sucursal_operativa_consulta_pero_no_reporta(self):
        self.assertIn(self.activo_payan.pk, activos_autorizados(self.dg).values_list("pk", flat=True))
        self.assertFalse(puede_reportar_activo(self.dg, self.activo_payan))


class PasaporteContenidoTests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.payan = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.proveedor = Proveedor.objects.create(nombre="Refrigeración del Valle")
        self.activo = Activo.objects.create(
            nombre="Cámara de refrigeración",
            sucursal=self.payan,
            ubicacion="Almacén",
            marca="ACME",
            modelo="HX-20",
            numero_serie="SER-1",
            costo_adquisicion=Decimal("100000.00"),
        )
        self.operativa = User.objects.create_user(username="encargada.payan")
        UserProfile.objects.create(user=self.operativa, sucursal=self.payan)
        self.dg = User.objects.create_superuser(username="dg", password="x")

        self.categoria = CategoriaFalla.objects.create(
            nombre="Refrigeración", tipo=CategoriaFalla.TIPO_EQUIPO
        )
        self.falla_abierta = ReporteFalla.objects.create(
            sucursal=self.payan,
            activo_relacionado=self.activo,
            categoria=self.categoria,
            titulo="No enfría",
            descripcion="Temperatura alta desde ayer",
            prioridad=ReporteFalla.PRIORIDAD_ALTA,
            justificacion_sin_foto="Sin cámara",
            reportado_por=self.operativa,
        )
        ReporteFalla.objects.create(
            sucursal=self.payan,
            activo_relacionado=self.activo,
            categoria=self.categoria,
            titulo="Ruido resuelto",
            descripcion="Ya se atendió",
            prioridad=ReporteFalla.PRIORIDAD_BAJA,
            estatus=ReporteFalla.ESTATUS_CERRADO,
            justificacion_sin_foto="Sin cámara",
            reportado_por=self.operativa,
        )
        self.orden = OrdenMantenimiento.objects.create(
            activo_ref=self.activo,
            tipo=OrdenMantenimiento.TIPO_CORRECTIVO,
            estatus=OrdenMantenimiento.ESTATUS_CERRADA,
            fecha_cierre=timezone.localdate(),
            costo_repuestos=Decimal("1500.00"),
            costo_mano_obra=Decimal("500.00"),
            proveedor_servicio=self.proveedor,
            numero_factura="F-001",
        )
        self.plan = PlanMantenimiento.objects.create(
            activo_ref=self.activo,
            nombre="Limpieza de condensador",
            proxima_ejecucion=timezone.localdate() + timedelta(days=15),
        )

    def test_identidad_y_seguimiento_siempre_presentes(self):
        pasaporte = construir_pasaporte(self.activo, self.operativa)

        self.assertEqual(pasaporte["activo"], self.activo)
        self.assertEqual(pasaporte["identidad"]["codigo"], self.activo.codigo)
        self.assertEqual(pasaporte["identidad"]["marca"], "ACME")
        self.assertEqual([f["id"] for f in pasaporte["fallas_abiertas"]], [self.falla_abierta.pk])
        self.assertEqual(pasaporte["fallas_abiertas"][0]["titulo"], "No enfría")
        self.assertIn(self.orden.folio, [o["folio"] for o in pasaporte["ordenes_recientes"]])
        self.assertEqual(pasaporte["proximo_plan"]["nombre"], self.plan.nombre)

    def test_operacion_no_recibe_costos_ni_facturas(self):
        pasaporte = construir_pasaporte(self.activo, self.operativa)

        self.assertFalse(pasaporte["puede_ver_costos"])
        self.assertNotIn("costos", pasaporte)
        self.assertNotIn("facturas", pasaporte)
        for orden in pasaporte["ordenes_recientes"]:
            self.assertNotIn("costo_total", orden)
            self.assertNotIn("numero_factura", orden)
            self.assertNotIn("proveedor_servicio", orden)

    def test_gestion_autorizada_si_recibe_costos_y_facturas(self):
        pasaporte = construir_pasaporte(self.activo, self.dg)

        self.assertTrue(pasaporte["puede_ver_costos"])
        self.assertEqual(pasaporte["costos"]["mantenimiento_total"], Decimal("2000.00"))
        self.assertEqual(pasaporte["costos"]["adquisicion"], Decimal("100000.00"))
        self.assertEqual([f["numero"] for f in pasaporte["facturas"]], ["F-001"])

    def test_eventos_recientes_topados_en_diez(self):
        for indice in range(14):
            OrdenMantenimiento.objects.create(
                activo_ref=self.activo, descripcion=f"Servicio {indice}"
            )
        pasaporte = construir_pasaporte(self.activo, self.dg)
        self.assertEqual(len(pasaporte["ordenes_recientes"]), 10)

    def test_url_qr_es_absoluta_y_lleva_el_token(self):
        request = self.factory.get("/", secure=True)
        url = url_qr_activo(request, self.activo)

        self.assertTrue(url.startswith("https://"))
        self.assertIn(f"/app/activos/q/{self.activo.qr_token}/", url)

    def test_svg_qr_contiene_svg_y_no_datos_del_activo(self):
        request = self.factory.get("/", secure=True)
        svg = svg_qr_activo(request, self.activo)

        self.assertIn("<svg", svg)
        self.assertNotIn(self.activo.nombre, svg)
        self.assertNotIn(self.activo.codigo, svg)
