from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from compras.models import OrdenCompra, SolicitudCompra
from maestros.models import CostoInsumo, Insumo, Proveedor, UnidadMedida


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
