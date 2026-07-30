import json
import base64
from types import SimpleNamespace

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core import mail
from django.urls import reverse

from activos.models import Activo
from core.models import Notificacion, Sucursal, UserModuleAccess, UserProfile
from fallas.models import CategoriaFalla, ReporteFalla
from fallas.serializers import ReporteFallaCreateSerializer


User = get_user_model()


class OperacionFallasApiTests(TestCase):
    def setUp(self):
        self.payan = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.leyva = Sucursal.objects.create(codigo="LEYVA", nombre="Leyva")
        self.user = User.objects.create_user(username="encargada.payan")
        UserProfile.objects.create(user=self.user, sucursal=self.payan)
        self.client.force_login(self.user)
        self.mantenimiento = User.objects.create_user(
            username="tecnico.mantenimiento", email="mantenimiento@example.com"
        )
        self.mantenimiento.groups.add(Group.objects.create(name="mantenimiento"))
        self.categoria_equipo = CategoriaFalla.objects.create(
            nombre="Refrigeración", tipo=CategoriaFalla.TIPO_EQUIPO
        )
        self.categoria_instalacion = CategoriaFalla.objects.create(
            nombre="Plomería", tipo=CategoriaFalla.TIPO_INSTALACION
        )
        self.activo_payan = Activo.objects.create(nombre="Refrigerador Payán", sucursal=self.payan)
        self.activo_leyva = Activo.objects.create(nombre="Refrigerador Leyva", sucursal=self.leyva)

    def _crear_reporte(self, *, sucursal, usuario, titulo):
        return ReporteFalla.objects.create(
            sucursal=sucursal,
            categoria=self.categoria_instalacion,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Sucursal",
            titulo=titulo,
            descripcion="Reporte visible en el historial operativo.",
            prioridad=ReporteFalla.PRIORIDAD_MEDIA,
            justificacion_sin_foto="Sin cámara disponible",
            reportado_por=usuario,
        )

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
        self.assertEqual(mail.outbox[0].to, ["mantenimiento@example.com"])

    def test_mermas_y_fallas_no_se_mezclan_en_la_misma_pantalla(self):
        merma = self.client.get(reverse("operacion:sucursal_tools"), {"tab": "mermas"})
        fallas = self.client.get(reverse("operacion:sucursal_tools"), {"tab": "fallas"})

        self.assertEqual(merma.status_code, 200)
        self.assertContains(merma, "Registrar merma de insumo")
        self.assertNotContains(merma, 'id="falla-form"')
        self.assertNotContains(merma, "Reportar falla")

        self.assertEqual(fallas.status_code, 200)
        self.assertContains(fallas, 'id="falla-form"')
        self.assertContains(fallas, "Historial de Payán")
        self.assertNotContains(fallas, 'id="merma-form"')
        self.assertNotContains(fallas, "Registrar merma de insumo")

    def test_historial_incluye_toda_la_sucursal_y_excluye_otras(self):
        companera = User.objects.create_user(
            username="companera.payan",
            first_name="Compañera",
            last_name="Payán",
        )
        UserProfile.objects.create(user=companera, sucursal=self.payan)
        usuario_leyva = User.objects.create_user(username="encargada.leyva")
        UserProfile.objects.create(user=usuario_leyva, sucursal=self.leyva)
        propia = self._crear_reporte(
            sucursal=self.payan,
            usuario=self.user,
            titulo="Refrigerador principal",
        )
        companera_reporte = self._crear_reporte(
            sucursal=self.payan,
            usuario=companera,
            titulo="Fuga en lavabo",
        )
        ajena = self._crear_reporte(
            sucursal=self.leyva,
            usuario=usuario_leyva,
            titulo="Puerta de Leyva",
        )

        response = self.client.get(reverse("operacion:sucursal_tools"), {"tab": "fallas"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Historial de Payán")
        self.assertContains(response, propia.titulo)
        self.assertContains(response, companera_reporte.titulo)
        self.assertContains(response, "Compañera Payán")
        self.assertNotContains(response, ajena.titulo)
        self.assertEqual(response.context["fallas_sucursal"].paginator.count, 2)

    def test_historial_de_sucursal_es_navegable_por_paginas(self):
        for index in range(11):
            self._crear_reporte(
                sucursal=self.payan,
                usuario=self.user,
                titulo=f"Falla histórica {index:02d}",
            )

        primera = self.client.get(reverse("operacion:sucursal_tools"), {"tab": "fallas"})
        segunda = self.client.get(
            reverse("operacion:sucursal_tools"),
            {"tab": "fallas", "fallas_page": 2},
        )

        self.assertEqual(primera.context["fallas_sucursal"].paginator.count, 11)
        self.assertEqual(primera.context["fallas_sucursal"].paginator.num_pages, 2)
        self.assertContains(primera, "Página 1 de 2")
        self.assertContains(primera, "fallas_page=2")
        self.assertContains(segunda, "Página 2 de 2")
        self.assertContains(segunda, "Falla histórica 00")

    def test_usuario_solo_mermas_puede_reportar_falla_de_su_sucursal(self):
        UserModuleAccess.objects.create(
            user=self.user,
            module="mermas.captura",
            access=UserModuleAccess.ACCESS_MANAGE,
        )

        response = self.client.post(
            reverse("operacion:fallas_crear_api"),
            data=json.dumps(
                {
                    "tipo_objetivo": "INSTALACION",
                    "area_instalacion": "Baño",
                    "categoria_id": self.categoria_instalacion.id,
                    "titulo": "Fuga desde App Operativa",
                    "descripcion": "Fuga menor",
                    "prioridad": "media",
                    "justificacion_sin_foto": "Sin cámara disponible",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertTrue(
            ReporteFalla.objects.filter(
                sucursal=self.payan,
                reportado_por=self.user,
                titulo="Fuga desde App Operativa",
            ).exists()
        )

    def test_post_html_regresa_al_formulario_con_fragmento_estable(self):
        response = self.client.post(
            reverse("operacion:fallas_crear_api"),
            {
                "tipo_objetivo": "INSTALACION", "area_instalacion": "Baño",
                "categoria_id": self.categoria_instalacion.id, "titulo": "Fuga",
                "descripcion": "Fuga menor", "prioridad": "media",
                "justificacion_sin_foto": "Sin cámara disponible",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.endswith("?tab=fallas#falla-form"))

    def test_error_html_conserva_borrador_y_regresa_al_formulario(self):
        response = self.client.post(
            reverse("operacion:fallas_crear_api"),
            {
                "tipo_objetivo": "INSTALACION", "area_instalacion": "Baño",
                "categoria_id": self.categoria_instalacion.id, "titulo": "Fuga conservada",
                "descripcion": "Fuga menor", "prioridad": "media", "justificacion_sin_foto": "",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.endswith("?tab=fallas#falla-form"))
        self.assertEqual(self.client.session["operacion_draft_fallas"]["titulo"], "Fuga conservada")

    def test_creador_pwa_existente_clasifica_sin_activo_como_instalacion(self):
        serializer = ReporteFallaCreateSerializer(
            data={
                "sucursal": self.payan.id, "categoria": self.categoria_instalacion.id,
                "titulo": "Falla histórica PWA", "descripcion": "Sin activo",
                "prioridad": "media",
                "foto_evidencia": SimpleUploadedFile(
                    "falla.png",
                    base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="),
                    content_type="image/png",
                ),
            },
            context={"request": SimpleNamespace(user=self.user)},
        )

        self.assertTrue(serializer.is_valid(), serializer.errors)
        reporte = serializer.save()
        self.assertEqual(reporte.tipo_objetivo, ReporteFalla.OBJETIVO_INSTALACION)
        self.assertEqual(reporte.area_instalacion, "Sucursal")
