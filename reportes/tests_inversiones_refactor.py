from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from django.contrib.auth.models import Group, User
from django.db import connection
from django.test import TestCase, override_settings
from django.urls import reverse

from reportes.models import (
    ProyectoInversion,
    ProyectoInversionEscenario,
    ProyectoInversionGasto,
    ProyectoInversionSnapshotMensual,
)


class InversionesRefactorTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            username="dg_inversiones",
            password="pass123",
            first_name="Dirección",
        )
        group, _ = Group.objects.get_or_create(name="DG")
        self.user.groups.add(group)
        self.client.login(username="dg_inversiones", password="pass123")

        self.guamuchil = ProyectoInversion.objects.create(
            id=1,
            nombre_proyecto="Apertura Guamúchil 2026",
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            fecha_inicio=date(2026, 1, 1),
            fecha_apertura=date(2026, 3, 24),
            estatus=ProyectoInversion.ESTATUS_EN_RECUPERACION,
            monto_inversion_planeado=Decimal("1200000.00"),
            monto_inversion_real=Decimal("1121753.85"),
            discount_rate=Decimal("12.00"),
            roi_objetivo=Decimal("25.00"),
            payback_objetivo_meses=24,
            metadata={
                "ubicacion": {
                    "ciudad": "Guamúchil",
                    "colonia": "Centro",
                    "m2": 80,
                    "descripcion": "Sucursal de referencia",
                },
                "supuestos_operativos": {
                    "renta": 12000,
                    "nomina": 85000,
                    "servicios": 18000,
                    "marketing": 12000,
                    "otros": 8000,
                    "gastos_fijos_total": 135000,
                    "ventas_base": 520000,
                    "margen_pct": 48,
                    "crecimiento_mensual_pct": 1.2,
                    "horizonte_meses": 36,
                },
            },
        )
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT setval(pg_get_serial_sequence(%s, %s), %s)",
                [ProyectoInversion._meta.db_table, "id", self.guamuchil.pk],
            )

        for idx in range(69):
            if idx < 2:
                categoria = ProyectoInversionGasto.CATEGORIA_OBRA_CIVIL
                monto = Decimal("357275.455")
            elif idx < 17:
                categoria = ProyectoInversionGasto.CATEGORIA_EQUIPAMIENTO
                monto = Decimal("21307.506")
            else:
                categoria = ProyectoInversionGasto.CATEGORIA_MOBILIARIO
                monto = Decimal("1684.43")
            ProyectoInversionGasto.objects.create(
                proyecto=self.guamuchil,
                fecha=date(2026, 1, 1),
                categoria=categoria,
                descripcion=f"Partida Guamúchil {idx + 1}",
                monto=monto,
                iva=Decimal("0.00"),
                monto_total=monto,
                referencia_contable=f"GML_TEST_{idx + 1:03d}",
            )

        for month in range(1, 6):
            ProyectoInversionSnapshotMensual.objects.create(
                proyecto=self.guamuchil,
                periodo=date(2026, month, 1),
                periodo_fin=date(2026, month, 28),
                ventas_mensuales=Decimal("520000.00"),
                utilidad_bruta=Decimal("249600.00"),
                gastos_operativos=Decimal("135000.00"),
                utilidad_operativa=Decimal("114600.00"),
                flujo_libre=Decimal("114600.00"),
                recuperacion_acumulada=Decimal(month) * Decimal("114600.00"),
                saldo_pendiente=Decimal("1121753.85") - Decimal(month) * Decimal("114600.00"),
                porcentaje_recuperado=Decimal(month) * Decimal("10.20"),
                roi_acumulado=Decimal(month) * Decimal("10.20"),
                payback_real_meses=Decimal("24.00"),
                data_source=ProyectoInversionSnapshotMensual.DATA_SOURCE_FACT,
            )

    def _payload_wizard(self):
        return {
            "action": "crear_proyecto",
            "nombre_proyecto": "Apertura Bamoa Refactor",
            "tipo_proyecto": ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            "ciudad": "Guasave",
            "colonia": "Bamoa",
            "m2_local": "45",
            "descripcion_ubicacion": "Local chico de pickup y punto de venta.",
            "fecha_inicio": "2026-06-01",
            "fecha_apertura": "2026-08-01",
            "partidas_json": json.dumps(
                [
                    {
                        "categoria": ProyectoInversionGasto.CATEGORIA_OBRA_CIVIL,
                        "descripcion": "Remodelación local Bamoa",
                        "proveedor_nombre": "Arquitecta",
                        "monto": "190000.00",
                        "iva": "0",
                    }
                ]
            ),
            "renta_mensual": "4000",
            "nomina_mensual": "18000",
            "servicios_mensual": "7000",
            "marketing_mensual": "2500",
            "otros_fijos_mensual": "1500",
            "ventas_promedio_base": "260000",
            "margen_bruto_pct": "45",
            "crecimiento_mensual_pct": "0.8",
            "horizonte_meses": "36",
            "discount_rate": "12",
            "roi_objetivo": "25",
            "payback_objetivo_meses": "24",
        }

    def test_portafolio_carga_200(self):
        response = self.client.get(reverse("reportes:inversiones_portafolio"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Proyectos de Inversión")
        self.assertContains(response, "Apertura Guamúchil 2026")

    def test_wizard_get_200(self):
        response = self.client.get(reverse("reportes:inversiones_wizard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Nuevo Proyecto de Inversión")
        self.assertContains(response, "partidas_json")

    def test_wizard_post_crea_proyecto_y_escenarios(self):
        response = self.client.post(reverse("reportes:inversiones_wizard"), self._payload_wizard())

        project = ProyectoInversion.objects.get(nombre_proyecto="Apertura Bamoa Refactor")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(project.monto_inversion_planeado, Decimal("190000.00"))
        self.assertEqual(project.gastos_inversion.count(), 1)
        self.assertEqual(project.escenarios.count(), 3)

    def test_detalle_get_tab_ficha(self):
        response = self.client.get(reverse("reportes:inversiones_detalle", args=[self.guamuchil.pk]), {"tab": "ficha"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ficha")
        self.assertContains(response, "Apertura Guamúchil 2026")

    def test_detalle_post_editar_ficha(self):
        response = self.client.post(
            reverse("reportes:inversiones_detalle", args=[self.guamuchil.pk]),
            {
                "action": "editar_ficha",
                "nombre_proyecto": "Apertura Guamúchil 2026 Ajustada",
                "tipo_proyecto": ProyectoInversion.TIPO_APERTURA_SUCURSAL,
                "fecha_apertura": "2026-03-24",
                "ciudad": "Guamúchil",
                "colonia": "Centro",
                "m2_local": "82",
                "renta_mensual": "12500",
                "deposito": "12500",
                "descripcion_ubicacion": "Ubicación validada",
                "competidores_conocidos": "Panaderías locales",
                "observaciones": "Ajuste de ficha",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.guamuchil.refresh_from_db()
        self.assertEqual(self.guamuchil.nombre_proyecto, "Apertura Guamúchil 2026 Ajustada")
        self.assertEqual(self.guamuchil.metadata["ubicacion"]["colonia"], "Centro")

    def test_detalle_post_add_expense(self):
        before = self.guamuchil.gastos_inversion.count()

        response = self.client.post(
            reverse("reportes:inversiones_detalle", args=[self.guamuchil.pk]),
            {
                "action": "add_expense",
                "fecha": "2026-02-01",
                "categoria": ProyectoInversionGasto.CATEGORIA_TECNOLOGIA,
                "descripcion": "Terminal adicional",
                "monto": "12000",
                "iva": "1920",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.guamuchil.gastos_inversion.count(), before + 1)
        self.assertTrue(self.guamuchil.gastos_inversion.filter(descripcion="Terminal adicional").exists())

    def test_detalle_post_actualizar_supuestos_recalcula_escenarios(self):
        response = self.client.post(
            reverse("reportes:inversiones_detalle", args=[self.guamuchil.pk]),
            {
                "action": "actualizar_supuestos",
                "renta_mensual": "12500",
                "nomina_mensual": "88000",
                "servicios_mensual": "19000",
                "marketing_mensual": "13000",
                "otros_fijos_mensual": "9000",
                "gastos_fijos_total": "141500",
                "ventas_promedio_base": "540000",
                "margen_bruto_pct": "48",
                "crecimiento_mensual_pct": "1.1",
                "horizonte_meses": "36",
                "discount_rate": "12",
                "roi_objetivo": "25",
                "payback_objetivo_meses": "24",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.guamuchil.refresh_from_db()
        self.assertEqual(self.guamuchil.escenarios.count(), 3)
        base = self.guamuchil.escenarios.get(tipo_escenario=ProyectoInversionEscenario.TIPO_BASE)
        self.assertEqual(base.ventas_promedio_mensuales, Decimal("540000.00"))
        self.assertEqual(self.guamuchil.metadata["supuestos_operativos"]["gastos_fijos_total"], 141500.0)

    def test_calcular_proyeccion_simple_tir_positiva(self):
        from reportes.investment_views import _calcular_proyeccion_simple

        proyeccion = _calcular_proyeccion_simple(
            self.guamuchil,
            {
                "ventas_base": 520000,
                "margen_pct": 48,
                "gastos_fijos_total": 135000,
                "crecimiento_mensual_pct": 1.2,
                "horizonte_meses": 36,
            },
            [],
        )

        self.assertTrue(proyeccion["disponible"])
        self.assertGreater(proyeccion["tir_anual"], 15)
        self.assertGreater(proyeccion["vpn"], 0)

    def test_calcular_proyeccion_simple_payback_correcto(self):
        from reportes.investment_views import _calcular_proyeccion_simple

        project = ProyectoInversion.objects.create(
            nombre_proyecto="Payback controlado",
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            fecha_inicio=date(2026, 6, 1),
            monto_inversion_planeado=Decimal("100000.00"),
            discount_rate=Decimal("12.00"),
        )
        proyeccion = _calcular_proyeccion_simple(
            project,
            {
                "ventas_base": 100000,
                "margen_pct": 50,
                "gastos_fijos_total": 30000,
                "crecimiento_mensual_pct": 0,
                "horizonte_meses": 12,
            },
            [],
        )

        self.assertEqual(proyeccion["payback_meses"], 5.0)

    @override_settings(OPENAI_API_KEY="")
    def test_generar_estudio_mercado_sin_api_key_retorna_error(self):
        from reportes.services_market_study import generar_estudio_mercado

        result = generar_estudio_mercado(
            ciudad="Guasave",
            colonia="Bamoa",
            descripcion_ubicacion="Local comercial",
        )

        self.assertIn("error", result)
        self.assertEqual(result["score_viabilidad"], 0)

    def test_guamuchil_pk1_intacto(self):
        self.assertEqual(self.guamuchil.pk, 1)
        self.assertEqual(self.guamuchil.gastos_inversion.count(), 69)
        self.assertEqual(self.guamuchil.snapshots_mensuales.count(), 5)
