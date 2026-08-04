import json
import csv
from decimal import Decimal
from io import StringIO
from tempfile import NamedTemporaryFile

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from activos.models import Activo
from core.models import Sucursal, UserModuleAccess
from mantenimiento.models import DetalleServicioMantenimiento, ServicioMantenimiento
from mantenimiento.services_grouped import create_grouped_service
from maestros.models import Proveedor


class GroupedMaintenanceServiceTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user("grouped-maintenance", password="test")
        UserModuleAccess.objects.create(user=cls.user, module="mantenimiento", access="manage")
        cls.branch = Sucursal.objects.create(codigo="GMS", nombre="Producción")
        cls.oven_a = Activo.objects.create(nombre="Horno Baxter 1", ubicacion="HORNOS", sucursal=cls.branch)
        cls.oven_b = Activo.objects.create(nombre="Horno Baxter 2", ubicacion="HORNOS", sucursal=cls.branch)
        cls.provider = Proveedor.objects.create(nombre="Manuel Técnico")

    def payload(self, **overrides):
        return {
            "fecha_servicio": "2026-05-20",
            "proveedor_nombre": "Manuel Técnico",
            "responsable": "Manuel",
            "numero_documento": "NOTA-123",
            "descripcion_general": "Mantenimiento general de hornos",
            "costo_total": "16800.00",
            "metodo_distribucion": ServicioMantenimiento.DISTRIBUCION_SIN_DESGLOSE,
            **overrides,
        }

    def test_one_invoice_creates_multiple_history_targets_without_duplicate_cost(self):
        service, created = create_grouped_service(
            payload=self.payload(),
            details=[
                {"tipo_objetivo": "ACTIVO", "activo_id": self.oven_a.pk, "trabajo_realizado": "Limpieza y sensores"},
                {"tipo_objetivo": "ACTIVO", "activo_id": self.oven_b.pk, "trabajo_realizado": "Cambio de contactor"},
                {
                    "tipo_objetivo": "INSTALACION", "sucursal_id": self.branch.pk,
                    "instalacion_categoria": "Eléctrico", "ubicacion": "Área de hornos",
                    "trabajo_realizado": "Ajuste de alimentación eléctrica",
                },
            ],
            user=self.user,
        )

        self.assertTrue(created)
        self.assertEqual(service.costo_total, Decimal("16800.00"))
        self.assertEqual(service.detalles.count(), 3)
        self.assertEqual(service.costo_asignado, Decimal("0"))
        self.assertFalse(Activo.objects.filter(nombre__icontains="instalación eléctrico").exists())

    def test_distribution_must_equal_document_total(self):
        with self.assertRaisesMessage(ValidationError, "debe coincidir"):
            create_grouped_service(
                payload=self.payload(metodo_distribucion=ServicioMantenimiento.DISTRIBUCION_REAL),
                details=[
                    {
                        "tipo_objetivo": "ACTIVO", "activo_id": self.oven_a.pk,
                        "trabajo_realizado": "Limpieza", "costo_asignado": "1000",
                    },
                ],
                user=self.user,
            )
        self.assertFalse(ServicioMantenimiento.objects.exists())

    def test_source_key_is_idempotent(self):
        payload = self.payload(clave_origen="BITACORA-2026-HORNOS-20")
        details = [{"tipo_objetivo": "ACTIVO", "activo_id": self.oven_a.pk, "trabajo_realizado": "Limpieza"}]
        first, created = create_grouped_service(payload=payload, details=details, user=self.user)
        second, created_again = create_grouped_service(payload=payload, details=details, user=self.user)
        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(first.pk, second.pk)
        self.assertEqual(ServicioMantenimiento.objects.count(), 1)

    def test_html_endpoint_accepts_mixed_grouped_service(self):
        self.client.force_login(self.user)
        details = [
            {"tipo_objetivo": "ACTIVO", "activo_id": self.oven_a.pk, "trabajo_realizado": "Limpieza"},
            {
                "tipo_objetivo": "INSTALACION", "sucursal_id": self.branch.pk,
                "instalacion_categoria": "Plomería", "ubicacion": "Producción",
                "trabajo_realizado": "Reparación de fuga",
            },
        ]
        response = self.client.post("/mantenimiento/servicios/crear/", {
            "modo_servicio": "realizado", "alcance": "activo", "sucursal_id": self.branch.pk,
            "activo_id": self.oven_a.pk, "fecha_objetivo": "2026-05-20",
            "descripcion": "Servicio agrupado", "costo_total": "16800",
            "proveedor_servicio": "Manuel Técnico", "detalles_json": json.dumps(details),
        })
        self.assertEqual(response.status_code, 302)
        service = ServicioMantenimiento.objects.get()
        self.assertEqual(service.detalles.count(), 2)
        self.assertEqual(
            service.detalles.filter(tipo_objetivo=DetalleServicioMantenimiento.OBJETIVO_INSTALACION).count(), 1,
        )

    def test_progressive_form_returns_json_and_preserves_context_contract(self):
        self.client.force_login(self.user)
        dashboard = self.client.get("/mantenimiento/")
        self.assertContains(dashboard, 'id="ordenServicioForm"')
        self.assertContains(dashboard, "data-async-action")
        self.assertContains(dashboard, 'data-pending-label="Guardando…"')

        response = self.client.post(
            "/mantenimiento/servicios/crear/",
            {
                "modo_servicio": "realizado", "alcance": "activo", "sucursal_id": self.branch.pk,
                "activo_id": self.oven_a.pk, "fecha_objetivo": "2026-05-20",
                "descripcion": "Servicio por respuesta progresiva", "costo_total": "900",
            },
            HTTP_ACCEPT="application/json",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertTrue(response.json()["redirect"].endswith("#tab-seguimiento"))

        invalid = self.client.post(
            "/mantenimiento/servicios/crear/",
            {"modo_servicio": "realizado", "alcance": "activo"},
            HTTP_ACCEPT="application/json",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(invalid.status_code, 400)
        self.assertFalse(invalid.json()["ok"])
        self.assertTrue(invalid.json()["toast"]["persistent"])

    def test_each_target_appears_in_history_without_repeating_document_cost(self):
        service, _created = create_grouped_service(
            payload=self.payload(),
            details=[
                {"tipo_objetivo": "ACTIVO", "activo_id": self.oven_a.pk, "trabajo_realizado": "Limpieza"},
                {"tipo_objetivo": "ACTIVO", "activo_id": self.oven_b.pk, "trabajo_realizado": "Contactores"},
            ],
            user=self.user,
        )
        self.client.force_login(self.user)
        history = self.client.get("/api/mantenimiento/v2/historial/?periodo=todo&tipo=servicio_general&estado=todo")
        self.assertEqual(history.status_code, 200)
        rows = history.json()["results"]
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["sujeto"]["label"] for row in rows}, {"Horno Baxter 1", "Horno Baxter 2"})
        self.assertTrue(all(row["costo"] is None for row in rows))

        detail_id = service.detalles.first().pk
        response = self.client.get(f"/api/mantenimiento/v2/items/servicio_general/{detail_id}/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["detalle"]["costo_documento"], "16800.00")

    def test_budget_index_counts_grouped_invoice_once(self):
        from datetime import date
        from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService

        create_grouped_service(
            payload=self.payload(),
            details=[
                {"tipo_objetivo": "ACTIVO", "activo_id": self.oven_a.pk, "trabajo_realizado": "Limpieza"},
                {"tipo_objetivo": "ACTIVO", "activo_id": self.oven_b.pk, "trabajo_realizado": "Contactores"},
            ],
            user=self.user,
        )
        rows = PresupuestoRealConsolidacionService._build_mant_equipo_index(date(2026, 5, 1))
        grouped_total = sum(row["monto"] for row in rows if row["activo_ref__sucursal__codigo"] == self.branch.codigo)
        self.assertEqual(grouped_total, Decimal("16800.00"))

    def test_importer_dry_run_then_apply_is_idempotent(self):
        columns = [
            "grupo_id", "fecha_servicio", "proveedor_id", "proveedor", "responsable", "numero_documento",
            "descripcion_general", "costo_total", "metodo_distribucion", "tipo_objetivo",
            "codigo_activo", "codigo_unidad", "codigo_sucursal", "instalacion_categoria",
            "ubicacion", "trabajo_realizado", "costo_asignado",
        ]
        with NamedTemporaryFile(mode="w", suffix=".csv", newline="", encoding="utf-8", delete=False) as temp:
            writer = csv.DictWriter(temp, fieldnames=columns)
            writer.writeheader()
            writer.writerow({
                "grupo_id": "HORNOS-001", "fecha_servicio": "2026-05-20",
                "proveedor_id": self.provider.pk, "proveedor": "Manuel Técnico",
                "numero_documento": "NOTA-123", "descripcion_general": "Mantenimiento general",
                "costo_total": "16800", "metodo_distribucion": "SIN_DESGLOSE", "tipo_objetivo": "ACTIVO",
                "codigo_activo": self.oven_a.codigo, "trabajo_realizado": "Limpieza",
            })
            writer.writerow({
                "grupo_id": "HORNOS-001", "fecha_servicio": "2026-05-20",
                "proveedor_id": self.provider.pk, "proveedor": "Manuel Técnico",
                "numero_documento": "NOTA-123", "descripcion_general": "Mantenimiento general",
                "costo_total": "16800", "metodo_distribucion": "SIN_DESGLOSE", "tipo_objetivo": "ACTIVO",
                "codigo_activo": self.oven_b.codigo, "trabajo_realizado": "Cambio de contactor",
            })
            path = temp.name

        output = StringIO()
        call_command("importar_servicios_mantenimiento", path, usuario=self.user.username, stdout=output)
        self.assertFalse(ServicioMantenimiento.objects.exists())
        self.assertIn("CARGA SIMULADA", output.getvalue())

        call_command(
            "importar_servicios_mantenimiento", path, usuario=self.user.username,
            apply=True, confirmar="CARGAR_MANTENIMIENTO", stdout=StringIO(),
        )
        self.assertEqual(ServicioMantenimiento.objects.count(), 1)
        self.assertEqual(ServicioMantenimiento.objects.get().detalles.count(), 2)

        output = StringIO()
        call_command(
            "importar_servicios_mantenimiento", path, usuario=self.user.username,
            apply=True, confirmar="CARGAR_MANTENIMIENTO", stdout=output,
        )
        self.assertEqual(ServicioMantenimiento.objects.count(), 1)
        self.assertIn("YA_EXISTE", output.getvalue())

    def test_importer_never_creates_provider_from_historical_text(self):
        columns = [
            "grupo_id", "fecha_servicio", "proveedor_id", "proveedor", "descripcion_general",
            "costo_total", "tipo_objetivo", "codigo_activo", "trabajo_realizado",
        ]
        with NamedTemporaryFile(mode="w", suffix=".csv", newline="", encoding="utf-8", delete=False) as temp:
            writer = csv.DictWriter(temp, fieldnames=columns)
            writer.writeheader()
            writer.writerow({
                "grupo_id": "PROVEEDOR-INVALIDO", "fecha_servicio": "2026-05-20",
                "proveedor": "Nombre escrito en bitácora", "descripcion_general": "Servicio",
                "costo_total": "500", "tipo_objetivo": "ACTIVO",
                "codigo_activo": self.oven_a.codigo, "trabajo_realizado": "Revisión",
            })
            path = temp.name

        errors = StringIO()
        with self.assertRaisesMessage(CommandError, "Validación fallida"):
            call_command(
                "importar_servicios_mantenimiento", path, usuario=self.user.username, stderr=errors,
            )
        self.assertIn("la importación no crea proveedores", errors.getvalue())
        self.assertFalse(Proveedor.objects.filter(nombre="Nombre escrito en bitácora").exists())
        self.assertFalse(ServicioMantenimiento.objects.exists())
