from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.access import ROLE_ALMACEN, ROLE_LECTURA
from core.models import Sucursal
from control.models import MermaPOS, VentaPOS
from inventario.models import ExistenciaInsumo
from maestros.models import Insumo, UnidadMedida
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, Receta


class ControlDiscrepanciasApiTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.admin = user_model.objects.create_superuser(
            username="admin_control_api",
            email="admin_control_api@example.com",
            password="test12345",
        )

        self.user_almacen = user_model.objects.create_user(
            username="almacen_control_api",
            email="almacen_control_api@example.com",
            password="test12345",
        )
        group_almacen, _ = Group.objects.get_or_create(name=ROLE_ALMACEN)
        self.user_almacen.groups.add(group_almacen)

        self.user_lectura = user_model.objects.create_user(
            username="lectura_control_api",
            email="lectura_control_api@example.com",
            password="test12345",
        )
        group_lectura, _ = Group.objects.get_or_create(name=ROLE_LECTURA)
        self.user_lectura.groups.add(group_lectura)

        self.sucursal = Sucursal.objects.create(codigo="SUC1", nombre="Sucursal 1", activa=True)

        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(
            nombre="Harina control",
            unidad_base=self.unidad,
            activo=True,
        )
        self.receta = Receta.objects.create(
            nombre="Pastel Control",
            hash_contenido="hash-control-001",
            codigo_point="PASTEL-CONTROL",
        )
        LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Harina control",
            cantidad=Decimal("2.000"),
            unidad=self.unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("10"),
            match_status=LineaReceta.STATUS_AUTO,
            match_score=100,
            match_method=LineaReceta.MATCH_EXACT,
        )

        self.plan = PlanProduccion.objects.create(
            nombre="Plan Control",
            fecha_produccion=timezone.localdate(),
            creado_por=self.admin,
        )
        PlanProduccionItem.objects.create(
            plan=self.plan,
            receta=self.receta,
            cantidad=Decimal("10.000"),
        )

        ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            stock_actual=Decimal("5.000"),
        )

    def test_import_ventas_pos_preview_y_confirm(self):
        self.client.force_login(self.user_almacen)
        payload = {
            "rows": [
                {
                    "receta_id": self.receta.id,
                    "codigo_point": "PASTEL-CONTROL",
                    "producto": "Pastel Control",
                    "sucursal_id": self.sucursal.id,
                    "fecha": str(timezone.localdate()),
                    "cantidad": "3",
                    "tickets": 2,
                    "monto_total": "420.50",
                }
            ]
        }

        resp_preview = self.client.post(
            reverse("api_control_ventas_pos_import_preview"),
            payload,
            content_type="application/json",
        )
        self.assertEqual(resp_preview.status_code, 200)
        body_preview = resp_preview.json()
        self.assertTrue(body_preview["preview"])
        self.assertEqual(body_preview["summary"]["applied"], 0)
        self.assertEqual(VentaPOS.objects.count(), 0)

        resp_confirm = self.client.post(
            reverse("api_control_ventas_pos_import_confirm"),
            payload,
            content_type="application/json",
        )
        self.assertEqual(resp_confirm.status_code, 200)
        body_confirm = resp_confirm.json()
        self.assertFalse(body_confirm["preview"])
        self.assertEqual(body_confirm["summary"]["applied"], 1)
        self.assertEqual(VentaPOS.objects.count(), 1)

    def test_import_mermas_pos_preview_y_confirm(self):
        self.client.force_login(self.user_almacen)
        payload = {
            "rows": [
                {
                    "receta_id": self.receta.id,
                    "codigo_point": "PASTEL-CONTROL",
                    "producto": "Pastel Control",
                    "sucursal_id": self.sucursal.id,
                    "fecha": str(timezone.localdate()),
                    "cantidad": "1",
                    "motivo": "Producto dañado",
                }
            ]
        }

        resp_preview = self.client.post(
            reverse("api_control_mermas_pos_import_preview"),
            payload,
            content_type="application/json",
        )
        self.assertEqual(resp_preview.status_code, 200)
        self.assertEqual(resp_preview.json()["summary"]["applied"], 0)
        self.assertEqual(MermaPOS.objects.count(), 0)

        resp_confirm = self.client.post(
            reverse("api_control_mermas_pos_import_confirm"),
            payload,
            content_type="application/json",
        )
        self.assertEqual(resp_confirm.status_code, 200)
        self.assertEqual(resp_confirm.json()["summary"]["applied"], 1)
        self.assertEqual(MermaPOS.objects.count(), 1)

    def test_discrepancias_endpoint_formula(self):
        VentaPOS.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            fecha=timezone.localdate(),
            codigo_point="PASTEL-CONTROL",
            producto_texto="Pastel Control",
            cantidad=Decimal("3.000"),
        )
        MermaPOS.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            fecha=timezone.localdate(),
            codigo_point="PASTEL-CONTROL",
            producto_texto="Pastel Control",
            cantidad=Decimal("1.000"),
        )

        self.client.force_login(self.user_lectura)
        resp = self.client.get(
            reverse("api_control_discrepancias"),
            {
                "from": str(timezone.localdate()),
                "to": str(timezone.localdate()),
                "sucursal_id": self.sucursal.id,
                "threshold_pct": "10",
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["totals"]["insumos"], 1)
        row = body["rows"][0]

        # produccion: 10 * 2 = 20
        self.assertAlmostEqual(row["produccion"], 20.0, places=3)
        # ventas: 3 * 2 = 6
        self.assertAlmostEqual(row["ventas_pos"], 6.0, places=3)
        # mermas: 1 * 2 = 2
        self.assertAlmostEqual(row["mermas_pos"], 2.0, places=3)
        # teorico = 20 - 6 - 2 = 12
        self.assertAlmostEqual(row["inventario_teorico"], 12.0, places=3)
        # real = 5 ; discrepancia = 5 - 12 = -7
        self.assertAlmostEqual(row["inventario_real"], 5.0, places=3)
        self.assertAlmostEqual(row["discrepancia"], -7.0, places=3)
        self.assertEqual(row["status"], "ALERTA")

    def test_discrepancias_requires_report_permission(self):
        user_model = get_user_model()
        no_role_user = user_model.objects.create_user(
            username="norole_control_api",
            email="norole_control_api@example.com",
            password="test12345",
        )
        self.client.force_login(no_role_user)
        resp = self.client.get(reverse("api_control_discrepancias"))
        self.assertEqual(resp.status_code, 403)
