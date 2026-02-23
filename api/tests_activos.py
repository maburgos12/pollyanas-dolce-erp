from datetime import timedelta

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento, PlanMantenimiento
from core.access import ROLE_LECTURA


class ActivosApiTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.admin = user_model.objects.create_superuser(
            username="admin_activos_api",
            email="admin_activos_api@example.com",
            password="test12345",
        )
        self.viewer = user_model.objects.create_user(
            username="viewer_activos_api",
            email="viewer_activos_api@example.com",
            password="test12345",
        )
        group_lectura, _ = Group.objects.get_or_create(name=ROLE_LECTURA)
        self.viewer.groups.add(group_lectura)

        today = timezone.localdate()
        self.activo_operativo = Activo.objects.create(
            nombre="Horno 1",
            estado=Activo.ESTADO_OPERATIVO,
            criticidad=Activo.CRITICIDAD_ALTA,
        )
        self.activo_mtto = Activo.objects.create(
            nombre="Batidora 1",
            estado=Activo.ESTADO_MANTENIMIENTO,
            criticidad=Activo.CRITICIDAD_MEDIA,
        )
        self.plan = PlanMantenimiento.objects.create(
            activo_ref=self.activo_operativo,
            nombre="Plan horno semanal",
            frecuencia_dias=7,
            proxima_ejecucion=today + timedelta(days=2),
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
        )

    def test_api_activos_disponibilidad(self):
        self.client.force_login(self.admin)
        resp = self.client.get(reverse("api_activos_disponibilidad"))
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["totales"]["activos"], 2)
        self.assertEqual(payload["totales"]["operativos"], 1)
        self.assertEqual(payload["totales"]["en_mantenimiento"], 1)
        self.assertEqual(payload["criticidad"]["ALTA"], 1)

    def test_api_activos_calendario_mantenimiento(self):
        self.client.force_login(self.admin)
        OrdenMantenimiento.objects.create(
            activo_ref=self.activo_operativo,
            plan_ref=self.plan,
            tipo=OrdenMantenimiento.TIPO_PREVENTIVO,
            prioridad=OrdenMantenimiento.PRIORIDAD_MEDIA,
            fecha_programada=timezone.localdate() + timedelta(days=1),
            descripcion="Prueba calendario",
            creado_por=self.admin,
        )
        resp = self.client.get(
            reverse("api_activos_calendario_mantenimiento"),
            {
                "from": str(timezone.localdate()),
                "to": str(timezone.localdate() + timedelta(days=7)),
            },
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertGreaterEqual(payload["totales"]["eventos"], 2)
        event_types = {row["tipo"] for row in payload["events"]}
        self.assertIn("PLAN", event_types)
        self.assertIn("ORDEN", event_types)

    def test_api_activos_crear_orden_y_cerrar_actualiza_plan(self):
        self.client.force_login(self.admin)
        create_resp = self.client.post(
            reverse("api_activos_ordenes"),
            {
                "activo_id": self.activo_operativo.id,
                "plan_id": self.plan.id,
                "tipo": OrdenMantenimiento.TIPO_PREVENTIVO,
                "prioridad": OrdenMantenimiento.PRIORIDAD_ALTA,
                "fecha_programada": str(timezone.localdate()),
                "responsable": "Mantenimiento",
                "descripcion": "Cambio de banda",
            },
            content_type="application/json",
        )
        self.assertEqual(create_resp.status_code, 201)
        orden_id = create_resp.json()["id"]

        en_proceso_resp = self.client.post(
            reverse("api_activos_orden_estatus", args=[orden_id]),
            {"estatus": OrdenMantenimiento.ESTATUS_EN_PROCESO},
            content_type="application/json",
        )
        self.assertEqual(en_proceso_resp.status_code, 200)

        cerrar_resp = self.client.post(
            reverse("api_activos_orden_estatus", args=[orden_id]),
            {"estatus": OrdenMantenimiento.ESTATUS_CERRADA},
            content_type="application/json",
        )
        self.assertEqual(cerrar_resp.status_code, 200)

        orden = OrdenMantenimiento.objects.get(pk=orden_id)
        self.plan.refresh_from_db()
        self.assertEqual(orden.estatus, OrdenMantenimiento.ESTATUS_CERRADA)
        self.assertIsNotNone(orden.fecha_inicio)
        self.assertIsNotNone(orden.fecha_cierre)
        self.assertEqual(self.plan.ultima_ejecucion, timezone.localdate())
        self.assertEqual(self.plan.proxima_ejecucion, timezone.localdate() + timedelta(days=7))
        self.assertGreaterEqual(BitacoraMantenimiento.objects.filter(orden=orden).count(), 3)

    def test_api_activos_create_requires_manage_permission(self):
        self.client.force_login(self.viewer)
        resp = self.client.post(
            reverse("api_activos_ordenes"),
            {
                "activo_id": self.activo_operativo.id,
                "descripcion": "Sin permiso",
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 403)

    def test_api_activos_view_endpoints_allow_viewer(self):
        self.client.force_login(self.viewer)
        resp_disponibilidad = self.client.get(reverse("api_activos_disponibilidad"))
        resp_calendario = self.client.get(reverse("api_activos_calendario_mantenimiento"))
        self.assertEqual(resp_disponibilidad.status_code, 200)
        self.assertEqual(resp_calendario.status_code, 200)
