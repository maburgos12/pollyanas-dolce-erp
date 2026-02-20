from datetime import date, datetime
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from compras.models import OrdenCompra, SolicitudCompra
from inventario.models import MovimientoInventario
from maestros.models import CostoInsumo, Insumo, Proveedor, UnidadMedida
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, Receta


class ComprasFase2FiltersTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_test",
            email="admin_test@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.proveedor = Proveedor.objects.create(nombre="Proveedor Test", activo=True)
        self.unidad_kg = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.unidad_lt = UnidadMedida.objects.create(
            codigo="lt",
            nombre="Litro",
            tipo=UnidadMedida.TIPO_VOLUMEN,
            factor_to_base=Decimal("1000"),
        )

        self.insumo_masa_blank = Insumo.objects.create(
            nombre="Harina sin categoria",
            categoria="",
            unidad_base=self.unidad_kg,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        self.insumo_masa_explicit = Insumo.objects.create(
            nombre="Mantequilla categoria masa",
            categoria="Masa",
            unidad_base=self.unidad_kg,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        self.insumo_volumen = Insumo.objects.create(
            nombre="Leche sin categoria",
            categoria="",
            unidad_base=self.unidad_lt,
            proveedor_principal=self.proveedor,
            activo=True,
        )

        CostoInsumo.objects.create(
            insumo=self.insumo_masa_blank,
            proveedor=self.proveedor,
            costo_unitario=Decimal("10"),
            source_hash="cost-harina-1",
        )
        CostoInsumo.objects.create(
            insumo=self.insumo_masa_explicit,
            proveedor=self.proveedor,
            costo_unitario=Decimal("5"),
            source_hash="cost-mantequilla-1",
        )
        CostoInsumo.objects.create(
            insumo=self.insumo_volumen,
            proveedor=self.proveedor,
            costo_unitario=Decimal("7"),
            source_hash="cost-leche-1",
        )

        self.periodo_mes = "2026-02"
        self.fecha_base = date(2026, 2, 10)
        self.solicitud_masa_blank = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="admin",
            insumo=self.insumo_masa_blank,
            proveedor_sugerido=self.proveedor,
            cantidad=Decimal("2"),
            fecha_requerida=self.fecha_base,
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        self.solicitud_masa_explicit = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="admin",
            insumo=self.insumo_masa_explicit,
            proveedor_sugerido=self.proveedor,
            cantidad=Decimal("1"),
            fecha_requerida=self.fecha_base,
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        self.solicitud_volumen = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="admin",
            insumo=self.insumo_volumen,
            proveedor_sugerido=self.proveedor,
            cantidad=Decimal("3"),
            fecha_requerida=self.fecha_base,
            estatus=SolicitudCompra.STATUS_APROBADA,
        )

        OrdenCompra.objects.create(
            solicitud=self.solicitud_masa_blank,
            proveedor=self.proveedor,
            fecha_emision=self.fecha_base,
            monto_estimado=Decimal("30"),
            estatus=OrdenCompra.STATUS_ENVIADA,
        )
        OrdenCompra.objects.create(
            solicitud=self.solicitud_volumen,
            proveedor=self.proveedor,
            fecha_emision=self.fecha_base,
            monto_estimado=Decimal("100"),
            estatus=OrdenCompra.STATUS_ENVIADA,
        )

        self.receta_plan = Receta.objects.create(
            nombre="Base prueba plan",
            hash_contenido="test-hash-plan-001",
        )
        LineaReceta.objects.create(
            receta=self.receta_plan,
            posicion=1,
            insumo=self.insumo_masa_blank,
            insumo_texto="Harina",
            cantidad=Decimal("2"),
            unidad=self.unidad_kg,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("10"),
            match_method=LineaReceta.MATCH_EXACT,
            match_score=100,
            match_status=LineaReceta.STATUS_AUTO,
        )
        self.plan = PlanProduccion.objects.create(
            nombre="Plan Febrero Test",
            fecha_produccion=self.fecha_base,
        )
        PlanProduccionItem.objects.create(
            plan=self.plan,
            receta=self.receta_plan,
            cantidad=Decimal("1"),
        )
        MovimientoInventario.objects.create(
            fecha=timezone.make_aware(datetime(2026, 2, 10, 11, 0, 0)),
            tipo=MovimientoInventario.TIPO_CONSUMO,
            insumo=self.insumo_masa_blank,
            cantidad=Decimal("3"),
            referencia=f"PLAN_PRODUCCION:{self.plan.id}",
        )

    def test_resumen_api_aplica_filtro_categoria(self):
        url = reverse("compras:solicitudes_resumen_api")
        response = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["filters"]["categoria"], "Masa")
        self.assertEqual(payload["totals"]["solicitudes_count"], 2)
        self.assertAlmostEqual(payload["totals"]["presupuesto_estimado_total"], 25.0, places=2)
        self.assertAlmostEqual(payload["totals"]["presupuesto_ejecutado_total"], 30.0, places=2)

        categorias = {row["categoria"]: row for row in payload["top_categorias"]}
        self.assertIn("Masa", categorias)
        self.assertAlmostEqual(categorias["Masa"]["estimado"], 25.0, places=2)
        self.assertAlmostEqual(categorias["Masa"]["ejecutado"], 30.0, places=2)

    def test_solicitudes_view_context_preserva_categoria(self):
        url = reverse("compras:solicitudes")
        response = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["categoria_filter"], "Masa")
        self.assertEqual(len(response.context["solicitudes"]), 2)
        self.assertIn("categoria=Masa", response.context["current_query"])

    def test_consumo_vs_plan_api_retorna_totales(self):
        url = reverse("compras:solicitudes_consumo_vs_plan_api")
        response = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
                "source": "all",
                "consumo_ref": "all",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["filters"]["categoria"], "Masa")
        self.assertEqual(payload["filters"]["consumo_ref"], "all")
        self.assertAlmostEqual(payload["totals"]["plan_qty_total"], 2.0, places=2)
        self.assertAlmostEqual(payload["totals"]["consumo_real_qty_total"], 3.0, places=2)
        self.assertAlmostEqual(payload["totals"]["plan_cost_total"], 20.0, places=2)
        self.assertAlmostEqual(payload["totals"]["consumo_real_cost_total"], 30.0, places=2)
        self.assertAlmostEqual(payload["totals"]["variacion_cost_total"], 10.0, places=2)
        self.assertIsNotNone(payload["totals"]["cobertura_pct"])
        self.assertEqual(payload["totals"]["sin_costo_count"], 0)
        self.assertEqual(payload["totals"]["semaforo_rojo_count"], 1)
        self.assertEqual(payload["rows"][0]["semaforo"], "ROJO")
        self.assertFalse(payload["rows"][0]["sin_costo"])

    def test_consumo_vs_plan_api_filtra_movimientos_solo_con_referencia_plan(self):
        MovimientoInventario.objects.create(
            fecha=timezone.make_aware(datetime(2026, 2, 10, 12, 0, 0)),
            tipo=MovimientoInventario.TIPO_CONSUMO,
            insumo=self.insumo_masa_blank,
            cantidad=Decimal("2"),
            referencia="SALIDA_MANUAL",
        )
        url = reverse("compras:solicitudes_consumo_vs_plan_api")
        payload_all = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
                "source": "all",
                "consumo_ref": "all",
            },
        ).json()
        payload_plan_ref = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
                "source": "all",
                "consumo_ref": "plan_ref",
            },
        ).json()

        self.assertAlmostEqual(payload_all["totals"]["consumo_real_qty_total"], 5.0, places=2)
        self.assertAlmostEqual(payload_plan_ref["totals"]["consumo_real_qty_total"], 3.0, places=2)
        self.assertEqual(payload_plan_ref["filters"]["consumo_ref"], "plan_ref")

    def test_consumo_vs_plan_marca_alerta_sin_costo_unitario(self):
        insumo_sin_costo = Insumo.objects.create(
            nombre="Insumo interno sin costo",
            categoria="Masa",
            unidad_base=self.unidad_kg,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        receta_sin_costo = Receta.objects.create(
            nombre="Receta sin costo",
            hash_contenido="test-hash-plan-002",
        )
        LineaReceta.objects.create(
            receta=receta_sin_costo,
            posicion=1,
            insumo=insumo_sin_costo,
            insumo_texto="Insumo interno sin costo",
            cantidad=Decimal("1"),
            unidad=self.unidad_kg,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("0"),
            match_method=LineaReceta.MATCH_EXACT,
            match_score=100,
            match_status=LineaReceta.STATUS_AUTO,
        )
        plan_sin_costo = PlanProduccion.objects.create(
            nombre="Plan Sin Costo",
            fecha_produccion=self.fecha_base,
        )
        PlanProduccionItem.objects.create(
            plan=plan_sin_costo,
            receta=receta_sin_costo,
            cantidad=Decimal("1"),
        )

        url = reverse("compras:solicitudes_consumo_vs_plan_api")
        payload = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
                "source": "all",
                "consumo_ref": "all",
            },
        ).json()

        self.assertGreaterEqual(payload["totals"]["sin_costo_count"], 1)
        row = next(r for r in payload["rows"] if r["insumo"] == "Insumo interno sin costo")
        self.assertTrue(row["sin_costo"])
        self.assertEqual(row["alerta"], "Sin costo unitario")
