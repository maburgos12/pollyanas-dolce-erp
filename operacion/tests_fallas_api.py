import json

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse

from activos.models import Activo
from core.models import Notificacion, Sucursal, UserProfile
from fallas.models import CategoriaFalla, ReporteFalla


User = get_user_model()


class OperacionFallasApiTests(TestCase):
    def setUp(self):
        self.payan = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.leyva = Sucursal.objects.create(codigo="LEYVA", nombre="Leyva")
        self.user = User.objects.create_user(username="encargada.payan")
        UserProfile.objects.create(user=self.user, sucursal=self.payan)
        self.client.force_login(self.user)
        self.mantenimiento = User.objects.create_user(username="tecnico.mantenimiento")
        self.mantenimiento.groups.add(Group.objects.create(name="mantenimiento"))
        self.categoria_equipo = CategoriaFalla.objects.create(
            nombre="Refrigeración", tipo=CategoriaFalla.TIPO_EQUIPO
        )
        self.categoria_instalacion = CategoriaFalla.objects.create(
            nombre="Plomería", tipo=CategoriaFalla.TIPO_INSTALACION
        )
        self.activo_payan = Activo.objects.create(nombre="Refrigerador Payán", sucursal=self.payan)
        self.activo_leyva = Activo.objects.create(nombre="Refrigerador Leyva", sucursal=self.leyva)

    def test_lista_activos_deriva_sucursal_de_sesion(self):
        response = self.client.get(reverse("operacion:fallas_activos_api"))

        self.assertEqual(response.status_code, 200)
        ids = {row["id"] for row in response.json()["activos"]}
        self.assertEqual(ids, {self.activo_payan.id})

    def test_rechaza_activo_de_otra_sucursal_aunque_cliente_envie_id(self):
        response = self.client.post(
            reverse("operacion:fallas_crear_api"),
            data=json.dumps(
                {
                    "tipo_objetivo": "EQUIPO",
                    "activo_id": self.activo_leyva.id,
                    "categoria_id": self.categoria_equipo.id,
                    "titulo": "No enfría",
                    "descripcion": "Temperatura alta",
                    "prioridad": "alta",
                    "justificacion_sin_foto": "Cámara no disponible",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(ReporteFalla.objects.exists())

    def test_instalacion_sin_foto_exige_justificacion_y_llega_mantenimiento(self):
        url = reverse("operacion:fallas_crear_api")
        payload = {
            "tipo_objetivo": "INSTALACION",
            "area_instalacion": "Baño",
            "categoria_id": self.categoria_instalacion.id,
            "titulo": "Fuga de agua",
            "descripcion": "Fuga debajo del lavabo",
            "prioridad": "media",
            "justificacion_sin_foto": "",
        }

        response = self.client.post(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 400)

        payload["justificacion_sin_foto"] = "La cámara del dispositivo no funcionó"
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(url, data=json.dumps(payload), content_type="application/json")

        self.assertEqual(response.status_code, 201)
        reporte = ReporteFalla.objects.get()
        self.assertEqual(reporte.sucursal, self.payan)
        self.assertIsNone(reporte.activo_relacionado)
        self.assertEqual(reporte.tipo_objetivo, ReporteFalla.OBJETIVO_INSTALACION)
        self.assertEqual(reporte.area_instalacion, "Baño")
        self.assertTrue(
            Notificacion.objects.filter(
                usuario=self.mantenimiento, objeto_tipo="ReporteFalla", objeto_id=str(reporte.id)
            ).exists()
        )
