"""Ninguna pantalla debe caerse porque el broker de Celery no responda.

El ERP encola avisos y refrescos desde vistas que ya escribieron en la base.
Cuando el broker no contesta, `.delay()` lanza `kombu OperationalError`; si nadie
lo atrapa, el usuario ve un 500 aunque su operación sí se completó, y la repite.

Nota sobre `on_commit`: dentro de `TestCase` la transacción nunca se confirma, así
que los callbacks registrados con `transaction.on_commit` no corren solos y la
prueba pasaría sin ejercitar nada. Por eso se usa `captureOnCommitCallbacks`.
"""

from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from kombu.exceptions import OperationalError

from core.access import ACCESS_MANAGE
from core.models import Sucursal, UserModuleAccess

User = get_user_model()

BROKER_CAIDO = OperationalError("Error 61 connecting to localhost:6379. Connection refused.")


class ReporteDeFallaSinBrokerTests(TestCase):
    """La sucursal debe poder levantar un reporte con el broker caído."""

    def setUp(self):
        from fallas.models import CategoriaFalla

        self.user = User.objects.create_user(username="pwa_falla", password="test12345")
        UserModuleAccess.objects.create(user=self.user, module="fallas", access=ACCESS_MANAGE)
        self.sucursal = Sucursal.objects.create(codigo="BRKQA", nombre="Sucursal broker", activa=True)
        self.categoria = CategoriaFalla.objects.create(
            nombre="Instalaciones", tipo=CategoriaFalla.TIPO_INSTALACION
        )
        self.client.force_login(self.user)

    def test_crear_reporte_no_falla_si_el_broker_no_responde(self):
        from fallas.models import ReporteFalla

        aviso = mock.Mock(side_effect=BROKER_CAIDO)
        with mock.patch("fallas.tasks.notificar_nuevo_reporte.delay", aviso):
            # execute=True fuerza los callbacks de on_commit, que es donde vive el aviso.
            with self.captureOnCommitCallbacks(execute=True):
                response = self.client.post(
                    "/api/fallas/reportes/",
                    {
                        "sucursal": self.sucursal.id,
                        "categoria": self.categoria.id,
                        "tipo_objetivo": ReporteFalla.OBJETIVO_INSTALACION,
                        "area_instalacion": "Piso de venta",
                        "titulo": "Gotera sobre la vitrina",
                        "descripcion": "Escurre agua del techo.",
                        "justificacion_sin_foto": "Sin batería en el teléfono.",
                    },
                    content_type="application/json",
                )

        self.assertTrue(aviso.called, "La prueba no ejercitó el encolado; revisa el mock.")
        self.assertEqual(response.status_code, 201, response.content[:400])
        self.assertTrue(ReporteFalla.objects.filter(titulo="Gotera sobre la vitrina").exists())


class RefrescoDeVentasSinBrokerTests(TestCase):
    """El refresco de ventas es accesorio: no puede propagar el error del broker."""

    def test_el_servicio_no_propaga_el_error_del_broker(self):
        from ventas.services.sales_freshness import queue_forecast_sales_refresh_if_needed

        encolar = mock.Mock(side_effect=BROKER_CAIDO)
        with mock.patch("ventas.services.sales_freshness.task_daily_sales_sync.delay", encolar):
            freshness = queue_forecast_sales_refresh_if_needed(triggered_by_id=None)

        self.assertTrue(encolar.called, "La prueba no ejercitó el encolado; revisa el mock.")
        self.assertFalse(freshness.is_fresh)


class ConsolidadoCedisSinBrokerTests(TestCase):
    """Generar el consolidado avisa del problema en vez de tronar la pantalla."""

    def setUp(self):
        self.user = User.objects.create_user(
            username="cedis_brk", password="test12345", is_superuser=True, is_staff=True
        )
        self.client.force_login(self.user)

    def test_generar_con_broker_caido_redirige_con_aviso(self):
        encolar = mock.Mock(side_effect=BROKER_CAIDO)
        with mock.patch(
            "recetas.views.consolidado_cedis.consolidado_nocturno_cedis.delay", encolar
        ):
            response = self.client.post(
                reverse("recetas:consolidado_cedis_generar"),
                {"fecha_operacion": "2026-09-10", "sincronizar_point": "1"},
                follow=True,
            )

        self.assertTrue(encolar.called, "La prueba no ejercitó el encolado; revisa el mock.")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            any("no se pudo" in str(m).lower() or "cola" in str(m).lower() for m in response.context["messages"]),
            "Debe explicarle al usuario que no se encoló nada.",
        )


class HorarioEspecialSinBrokerTests(TestCase):
    """Reintentar no debe dejar la solicitud aprobada sin nada que la ejecute."""

    def setUp(self):
        self.user = User.objects.create_user(
            username="horarios_brk", password="test12345", is_superuser=True, is_staff=True
        )
        self.sucursal = Sucursal.objects.create(codigo="HORQA", nombre="Sucursal horarios", activa=True)
        self.client.force_login(self.user)

    def test_reintentar_con_broker_caido_no_deja_la_solicitud_aprobada_a_medias(self):
        from horarios_especiales.models import SolicitudHorarioEspecial

        obj = SolicitudHorarioEspecial.objects.create(
            raw_command="Abrir Nochebuena de 8 a 14 en Sucursal horarios",
            reason="Nochebuena",
            status=SolicitudHorarioEspecial.STATUS_FALLIDO,
            requested_by=self.user,
        )
        encolar = mock.Mock(side_effect=BROKER_CAIDO)
        with mock.patch("api.special_hours_views.execute_special_hours_request_task.delay", encolar):
            response = self.client.post(
                reverse("api_integraciones_special_hours_retry", args=[obj.id])
            )

        obj.refresh_from_db()
        self.assertTrue(encolar.called, "La prueba no ejercitó el encolado; revisa el mock.")
        if response.status_code >= 400:
            self.assertNotEqual(
                obj.status,
                SolicitudHorarioEspecial.STATUS_APROBADO,
                "Quedó aprobada sin nada encolado que la ejecute.",
            )
