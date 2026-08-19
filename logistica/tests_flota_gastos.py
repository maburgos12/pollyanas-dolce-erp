from datetime import timedelta
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.messages import get_messages
from django.core.files.base import ContentFile
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal
from logistica.models import ReparacionUnidad, ServicioRealizadoUnidad, TipoServicioUnidad, Unidad
from logistica.services_flota import resumen_anual_unidad


class FlotaGastosResumenTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.sucursal = Sucursal.objects.create(codigo="FG-01", nombre="Flota gastos")
        cls.unidad = Unidad.objects.create(codigo="FG-U1", descripcion="Unidad de prueba", sucursal=cls.sucursal)
        cls.tipo_servicio = TipoServicioUnidad.objects.create(
            nombre="Servicio ordinario",
            tipo_intervalo=TipoServicioUnidad.INTERVALO_TIEMPO,
        )
        cls.tipo_inicial = TipoServicioUnidad.objects.create(
            nombre="Registro inicial de kilometraje",
            tipo_intervalo=TipoServicioUnidad.INTERVALO_KM,
            activo=False,
        )

    def test_suma_servicios_y_reparaciones_sin_duplicar(self):
        today = timezone.localdate()
        ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo_servicio,
            fecha_servicio=today,
            costo=Decimal("6898.00"),
        )
        ReparacionUnidad.objects.create(
            unidad=self.unidad,
            fecha_ingreso=today,
            descripcion_falla="Balero",
            costo_total=Decimal("1500.00"),
        )

        resumen = resumen_anual_unidad(self.unidad, year=today.year, today=today)

        self.assertEqual(resumen.servicios_cantidad, 1)
        self.assertEqual(resumen.servicios_total, Decimal("6898.00"))
        self.assertEqual(resumen.reparaciones_cantidad, 1)
        self.assertEqual(resumen.reparaciones_total, Decimal("1500.00"))
        self.assertEqual(resumen.gasto_total, Decimal("8398.00"))

    def test_excluye_anulados_futuros_y_registro_inicial(self):
        today = timezone.localdate()
        actor = get_user_model().objects.create_user("flota-auditor")
        ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo_servicio,
            fecha_servicio=today,
            costo=Decimal("400.00"),
        )
        ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo_servicio,
            fecha_servicio=today - timedelta(days=1),
            costo=Decimal("900.00"),
            anulado_en=timezone.now(),
            anulado_por=actor,
            motivo_anulacion="Prueba",
        )
        ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo_servicio,
            fecha_servicio=today + timedelta(days=1),
            costo=Decimal("800.00"),
        )
        ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo_inicial,
            fecha_servicio=today,
            costo=Decimal("700.00"),
        )

        resumen = resumen_anual_unidad(self.unidad, year=today.year, today=today)

        self.assertEqual(resumen.servicios_cantidad, 1)
        self.assertEqual(resumen.servicios_total, Decimal("400.00"))
        self.assertEqual(resumen.gasto_total, Decimal("400.00"))


class FlotaResumenViewTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.usuario = get_user_model().objects.create_superuser(
            username="flota-resumen-admin",
            email="flota-resumen@example.com",
            password="test",
        )
        cls.sucursal = Sucursal.objects.create(codigo="FR-01", nombre="Resumen flota")
        cls.unidad = Unidad.objects.create(codigo="FR-U1", descripcion="Unidad resumen", sucursal=cls.sucursal)
        cls.tipo = TipoServicioUnidad.objects.create(
            nombre="Suspensión",
            tipo_intervalo=TipoServicioUnidad.INTERVALO_TIEMPO,
        )

    def test_resumen_muestra_servicios_reparaciones_y_total(self):
        today = timezone.localdate()
        ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo,
            fecha_servicio=today,
            costo=Decimal("6898.00"),
        )
        ReparacionUnidad.objects.create(
            unidad=self.unidad,
            fecha_ingreso=today,
            descripcion_falla="Balero",
            costo_total=Decimal("1500.00"),
        )
        self.client.force_login(self.usuario)

        response = self.client.get(reverse("logistica:flota_resumen"))

        row = next(item for item in response.context["unidades_resumen"] if item["unidad"] == self.unidad)
        self.assertEqual(row["servicios_anio"], 1)
        self.assertEqual(row["gasto_servicios_anio"], Decimal("6898.00"))
        self.assertEqual(row["reparaciones_anio"], 1)
        self.assertEqual(row["gasto_reparaciones_anio"], Decimal("1500.00"))
        self.assertEqual(row["gasto_total_anio"], Decimal("8398.00"))
        self.assertContains(response, "Servicios: 1 · $6,898.00")
        self.assertContains(response, "Reparaciones: 1 · $1,500.00")
        self.assertContains(response, "Total $8,398.00")

    def test_servicio_futuro_no_es_el_ultimo_realizado(self):
        today = timezone.localdate()
        pasado = ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo,
            fecha_servicio=today - timedelta(days=2),
            costo=Decimal("250.00"),
        )
        ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo,
            fecha_servicio=today + timedelta(days=30),
            costo=Decimal("500.00"),
        )
        self.client.force_login(self.usuario)

        response = self.client.get(reverse("logistica:flota_resumen"))

        row = next(item for item in response.context["unidades_resumen"] if item["unidad"] == self.unidad)
        self.assertEqual(row["ultimo_servicio"], pasado)


class FechaServicioRealizadoTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.usuario = get_user_model().objects.create_superuser(
            username="flota-fecha-admin",
            email="flota-fecha@example.com",
            password="test",
        )
        cls.sucursal = Sucursal.objects.create(codigo="FF-01", nombre="Fechas flota")
        cls.unidad = Unidad.objects.create(codigo="FF-U1", descripcion="Unidad fechas", sucursal=cls.sucursal)
        cls.tipo = TipoServicioUnidad.objects.create(
            nombre="Cambio de aceite",
            tipo_intervalo=TipoServicioUnidad.INTERVALO_TIEMPO,
        )

    def _mensajes(self, response):
        return [str(message) for message in get_messages(response.wsgi_request)]

    def test_detalle_unidad_rechaza_servicio_realizado_futuro(self):
        self.client.force_login(self.usuario)
        response = self.client.post(
            reverse("logistica:unidad_servicio_nuevo", kwargs={"pk": self.unidad.pk}),
            {
                "tipo_servicio": self.tipo.pk,
                "fecha_servicio": (timezone.localdate() + timedelta(days=1)).isoformat(),
                "costo": "500.00",
            },
        )

        self.assertFalse(ServicioRealizadoUnidad.objects.filter(unidad=self.unidad).exists())
        self.assertIn("La fecha de un servicio realizado no puede estar en el futuro.", self._mensajes(response))

    def test_mantenimiento_rechaza_servicio_realizado_futuro(self):
        self.client.force_login(self.usuario)
        response = self.client.post(
            reverse("mantenimiento:mant-flota-servicio"),
            {
                "unidad_id": self.unidad.pk,
                "nombre_servicio": "Servicio futuro",
                "fecha_servicio": (timezone.localdate() + timedelta(days=1)).isoformat(),
                "modo_servicio": "realizado",
                "costo": "500.00",
            },
        )

        self.assertFalse(ServicioRealizadoUnidad.objects.filter(unidad=self.unidad).exists())
        self.assertIn("La fecha de un servicio realizado no puede estar en el futuro.", self._mensajes(response))

    def test_mantenimiento_permite_programar_proxima_fecha(self):
        self.client.force_login(self.usuario)
        response = self.client.post(
            reverse("mantenimiento:mant-flota-servicio"),
            {
                "unidad_id": self.unidad.pk,
                "nombre_servicio": "Servicio programado",
                "fecha_servicio": timezone.localdate().isoformat(),
                "modo_servicio": "programado",
                "proxima_fecha": (timezone.localdate() + timedelta(days=30)).isoformat(),
            },
        )

        self.assertEqual(response.status_code, 302)
        servicio = ServicioRealizadoUnidad.objects.get(unidad=self.unidad)
        self.assertEqual(servicio.proxima_fecha, timezone.localdate() + timedelta(days=30))


class CorregirHistorialCheyenneCommandTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_user("auditor-cheyenne")
        sucursal = Sucursal.objects.create(codigo="CH-01", nombre="Cheyenne")
        self.unidad = Unidad.objects.create(codigo="GS-CH1", descripcion="Chevrolet Cheyenne", sucursal=sucursal)
        self.tipo_incorrecto = TipoServicioUnidad.objects.create(
            nombre="Suspensión", tipo_intervalo=TipoServicioUnidad.INTERVALO_TIEMPO
        )
        self.tipo_correcto = TipoServicioUnidad.objects.create(
            nombre="Reparación correctiva", tipo_intervalo=TipoServicioUnidad.INTERVALO_TIEMPO
        )
        self.futuro = ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo_incorrecto,
            fecha_servicio="2026-10-28",
            costo=Decimal("0"),
        )
        self.suspension = ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo_incorrecto,
            fecha_servicio="2026-08-10",
            costo=Decimal("6898.00"),
            archivo_factura=ContentFile(b"factura", name="factura.pdf"),
        )

    def _command(self, *, apply=False):
        args = {
            "actor_username": self.actor.username,
            "servicio_futuro_id": self.futuro.pk,
            "servicio_suspension_id": self.suspension.pk,
        }
        if apply:
            args["apply"] = True
        call_command("corregir_historial_cheyenne", **args)

    def test_simulacion_no_modifica(self):
        self._command()
        self.futuro.refresh_from_db()
        self.suspension.refresh_from_db()
        self.assertIsNone(self.futuro.anulado_en)
        self.assertEqual(self.suspension.tipo_servicio, self.tipo_incorrecto)

    def test_aplica_y_es_idempotente_preservando_factura_e_importes(self):
        factura = self.suspension.archivo_factura.name
        self._command(apply=True)
        self._command(apply=True)
        self.futuro.refresh_from_db()
        self.suspension.refresh_from_db()
        self.assertEqual(self.futuro.motivo_anulacion, "Fecha futura y servicio no confirmado")
        self.assertEqual(self.futuro.anulado_por, self.actor)
        self.assertEqual(self.suspension.tipo_servicio, self.tipo_correcto)
        self.assertEqual(self.suspension.costo, Decimal("6898.00"))
        self.assertEqual(self.suspension.archivo_factura.name, factura)

    def test_aborta_si_no_coincide_el_importe(self):
        ServicioRealizadoUnidad.objects.filter(pk=self.suspension.pk).update(costo=Decimal("1.00"))
        with self.assertRaises(CommandError):
            self._command(apply=True)
