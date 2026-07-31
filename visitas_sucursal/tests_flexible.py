from datetime import date
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal
from logistica.models import PuntoLogistico

from .models import ChecklistVisita, VisitaSucursal


class VisitaSucursalFlexibleModelTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)

    def test_extraordinaria_no_finge_fecha_programada(self):
        visita = VisitaSucursal(
            sucursal=self.sucursal,
            tipo=VisitaSucursal.TIPO_EXTRAORDINARIA,
            estatus=VisitaSucursal.ESTATUS_BORRADOR,
            fecha_programada=None,
            motivo_extraordinaria=VisitaSucursal.MOTIVO_QUEJA,
            detalle_extraordinaria="Queja por atención en mostrador.",
        )

        visita.full_clean()
        visita.save()

        self.assertIsNone(visita.fecha_programada)
        self.assertIn("Extraordinaria", str(visita))

    def test_visita_planeada_sigue_requiriendo_fecha(self):
        visita = VisitaSucursal(
            sucursal=self.sucursal,
            tipo=VisitaSucursal.TIPO_QUINCENAL,
            fecha_programada=None,
        )

        with self.assertRaisesMessage(ValidationError, "fecha programada"):
            visita.full_clean()

    def test_extraordinaria_requiere_causa_y_detalle(self):
        visita = VisitaSucursal(
            sucursal=self.sucursal,
            tipo=VisitaSucursal.TIPO_EXTRAORDINARIA,
            estatus=VisitaSucursal.ESTATUS_BORRADOR,
            fecha_programada=None,
        )

        with self.assertRaises(ValidationError) as error:
            visita.full_clean()

        self.assertIn("motivo_extraordinaria", error.exception.message_dict)
        self.assertIn("detalle_extraordinaria", error.exception.message_dict)

    def test_desviacion_dias_conserva_la_fecha_planeada(self):
        visita = VisitaSucursal(
            sucursal=self.sucursal,
            fecha_programada=date(2026, 8, 10),
            fecha_real=date(2026, 8, 12),
            estatus=VisitaSucursal.ESTATUS_REALIZADA,
        )

        self.assertEqual(visita.desviacion_dias, 2)
        self.assertEqual(visita.fecha_programada, date(2026, 8, 10))


class AuditoriaSucursalFlexibleAppTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser("admin", "admin@example.com", "pass")
        self.sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        self.otra = Sucursal.objects.create(codigo="CTR", nombre="Centro", activa=True)
        self.client.force_login(self.user)

    def test_seleccionar_sucursal_muestra_solo_programaciones_pendientes(self):
        pendiente = VisitaSucursal.objects.create(
            sucursal=self.sucursal,
            fecha_programada="2026-08-10",
            creado_por=self.user,
        )
        VisitaSucursal.objects.create(
            sucursal=self.sucursal,
            fecha_programada="2026-08-05",
            estatus=VisitaSucursal.ESTATUS_REALIZADA,
            fecha_real="2026-08-06",
            creado_por=self.user,
        )
        VisitaSucursal.objects.create(
            sucursal=self.otra,
            fecha_programada="2026-08-09",
            creado_por=self.user,
        )

        response = self.client.get(reverse("visitas_sucursal:app"), {"sucursal": self.sucursal.id})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(list(response.context["programaciones_pendientes"]), [pendiente])
        self.assertEqual(response.context["filtro_sucursal"], self.sucursal)
        self.assertContains(response, "Elige la visita que estás cumpliendo")

    def test_iniciar_extraordinaria_crea_borrador_con_motivo(self):
        token = uuid4()

        response = self.client.post(
            reverse("visitas_sucursal:app"),
            {
                "action": "iniciar_extraordinaria",
                "sucursal": self.sucursal.id,
                "motivo_extraordinaria": VisitaSucursal.MOTIVO_QUEJA,
                "detalle_extraordinaria": "Queja por servicio.",
                "clave_idempotencia": str(token),
            },
        )

        visita = VisitaSucursal.objects.get(clave_idempotencia=token)
        self.assertRedirects(
            response,
            f"{reverse('visitas_sucursal:app')}?sucursal={self.sucursal.id}&visita={visita.id}",
            fetch_redirect_response=False,
        )
        self.assertEqual(visita.estatus, VisitaSucursal.ESTATUS_BORRADOR)
        self.assertEqual(visita.motivo_extraordinaria, VisitaSucursal.MOTIVO_QUEJA)

    def test_iniciar_extraordinaria_sin_detalle_conserva_contexto(self):
        response = self.client.post(
            reverse("visitas_sucursal:app"),
            {
                "action": "iniciar_extraordinaria",
                "sucursal": self.sucursal.id,
                "motivo_extraordinaria": VisitaSucursal.MOTIVO_OTRO,
                "detalle_extraordinaria": "",
                "clave_idempotencia": str(uuid4()),
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn(f"sucursal={self.sucursal.id}", response.url)
        self.assertFalse(VisitaSucursal.objects.filter(tipo=VisitaSucursal.TIPO_EXTRAORDINARIA).exists())

    def test_formulario_de_programacion_no_ofrece_extraordinaria(self):
        response = self.client.get(reverse("visitas_sucursal:nueva"))

        tipos = dict(response.context["tipo_choices"])
        self.assertNotIn(VisitaSucursal.TIPO_EXTRAORDINARIA, tipos)

    def test_error_gps_async_conserva_visita_pendiente_para_reintento(self):
        visita = VisitaSucursal.objects.create(
            sucursal=self.sucursal,
            fecha_programada=date(2026, 8, 10),
            creado_por=self.user,
        )
        item = ChecklistVisita.objects.create(
            visita=visita,
            categoria="Orden y limpieza",
            titulo="Pisos",
            orden=1,
        )
        PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Payán",
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=80,
        )

        response = self.client.post(
            reverse("visitas_sucursal:app"),
            {
                "action": "ejecutar",
                "sucursal": self.sucursal.id,
                "visita_id": visita.id,
                f"respuesta_{item.id}": ChecklistVisita.RESPUESTA_SI,
                "gps_latitud": "25.570000",
                "gps_longitud": "-108.470000",
                "gps_precision_m": "100.01",
            },
            HTTP_ACCEPT="application/json",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(response.status_code, 422)
        self.assertFalse(response.json()["ok"])
        self.assertIn("precisión", response.json()["toast"]["message"])
        visita.refresh_from_db()
        item.refresh_from_db()
        self.assertEqual(visita.estatus, VisitaSucursal.ESTATUS_PROGRAMADA)
        self.assertEqual(item.respuesta, ChecklistVisita.RESPUESTA_PENDIENTE)


class CronogramaFlexibleTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser("admin", "admin@example.com", "pass")
        self.sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        self.client.force_login(self.user)

    def test_conserva_plan_y_muestra_ejecucion_en_el_dia_real(self):
        visita = VisitaSucursal.objects.create(
            sucursal=self.sucursal,
            fecha_programada=date(2026, 8, 10),
            fecha_real=date(2026, 8, 12),
            estatus=VisitaSucursal.ESTATUS_REALIZADA,
            creado_por=self.user,
        )

        response = self.client.get(reverse("visitas_sucursal:lista"), {"anio": 2026, "mes": 8})

        row = next(item for item in response.context["rows"] if item["sucursal"] == self.sucursal)
        day_10 = row["cells"][9]
        day_12 = row["cells"][11]
        self.assertEqual(day_10["planeadas"], [visita])
        self.assertEqual(day_12["ejecutadas"], [visita])
        self.assertEqual(response.context["total_plan_mes"], 1)
        self.assertEqual(response.context["total_ejecutadas_mes"], 1)
        self.assertEqual(response.context["avance_real"], 100)
        self.assertContains(response, "Programada 10/08")
        self.assertContains(response, "Ejecutada 12/08")

    def test_extraordinaria_aparece_como_actividad_sin_aumentar_el_plan(self):
        visita = VisitaSucursal.objects.create(
            sucursal=self.sucursal,
            tipo=VisitaSucursal.TIPO_EXTRAORDINARIA,
            estatus=VisitaSucursal.ESTATUS_REALIZADA,
            fecha_programada=None,
            fecha_real=date(2026, 8, 15),
            motivo_extraordinaria=VisitaSucursal.MOTIVO_QUEJA,
            detalle_extraordinaria="Seguimiento a una queja de servicio.",
            creado_por=self.user,
        )

        response = self.client.get(reverse("visitas_sucursal:lista"), {"anio": 2026, "mes": 8})

        row = next(item for item in response.context["rows"] if item["sucursal"] == self.sucursal)
        self.assertEqual(row["cells"][14]["ejecutadas"], [visita])
        self.assertEqual(response.context["total_plan_mes"], 0)
        self.assertEqual(response.context["total_ejecutadas_mes"], 0)
        self.assertEqual(response.context["extraordinarias_mes"], 1)
        self.assertEqual(response.context["avance_real"], 0)
        self.assertContains(response, "Extraordinaria ejecutada 15/08")
