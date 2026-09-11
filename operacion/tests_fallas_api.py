import json
import base64
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import quote

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core import mail
from django.urls import reverse

from django.utils import timezone

from activos.models import Activo, OrdenMantenimiento
from core.models import AuditLog, Notificacion, Sucursal, UserModuleAccess, UserProfile
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


class ActivoPasaporteQrTests(TestCase):
    """Ficha abierta desde la etiqueta QR: sesión, alcance y costos."""

    def setUp(self):
        self.payan = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.leyva = Sucursal.objects.create(codigo="LEYVA", nombre="Leyva")
        self.activo_payan = Activo.objects.create(
            nombre="Refrigerador Payán", sucursal=self.payan, ubicacion="Mostrador"
        )
        self.activo_leyva = Activo.objects.create(nombre="Refrigerador Leyva", sucursal=self.leyva)
        self.encargada = User.objects.create_user(username="encargada.payan")
        UserProfile.objects.create(user=self.encargada, sucursal=self.payan)
        self.dg = User.objects.create_superuser(username="dg.pasaporte", password="x")

        OrdenMantenimiento.objects.create(
            activo_ref=self.activo_payan,
            descripcion="Cambio de compresor",
            estatus=OrdenMantenimiento.ESTATUS_CERRADA,
            fecha_cierre=timezone.localdate(),
            costo_repuestos=Decimal("7654.32"),
            numero_factura="FAC-QR-001",
        )

    def _url(self, activo):
        return reverse("operacion:activo_pasaporte", args=[activo.qr_token])

    def test_anonimo_va_a_login_conservando_el_activo_pedido(self):
        destino = self._url(self.activo_payan)
        response = self.client.get(destino)

        self.assertEqual(response.status_code, 302)
        self.assertIn(f"next={quote(destino)}", response["Location"])

    def test_encargada_abre_el_pasaporte_de_su_sucursal(self):
        self.client.force_login(self.encargada)
        response = self.client.get(self._url(self.activo_payan))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.activo_payan.codigo)
        self.assertContains(response, "Refrigerador Payán")

    def test_activo_de_otra_sucursal_no_existe_para_la_encargada(self):
        self.client.force_login(self.encargada)
        response = self.client.get(self._url(self.activo_leyva))

        self.assertEqual(response.status_code, 404)

    def test_token_inventado_no_revela_nada(self):
        self.client.force_login(self.encargada)
        response = self.client.get(
            reverse("operacion:activo_pasaporte", args=["11111111-1111-4111-8111-111111111111"])
        )

        self.assertEqual(response.status_code, 404)

    def test_operacion_no_recibe_costos_ni_facturas_en_el_html(self):
        self.client.force_login(self.encargada)
        response = self.client.get(self._url(self.activo_payan))
        cuerpo = response.content.decode()

        self.assertNotIn("7654.32", cuerpo)
        self.assertNotIn("7,654.32", cuerpo)
        self.assertNotIn("FAC-QR-001", cuerpo)

    def test_gestion_autorizada_si_ve_costos_del_activo(self):
        self.client.force_login(self.dg)
        response = self.client.get(self._url(self.activo_payan))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "FAC-QR-001")

    def test_apertura_queda_en_bitacora_sin_datos_personales(self):
        self.client.force_login(self.encargada)
        self.client.get(self._url(self.activo_payan))

        registro = AuditLog.objects.filter(action="SCAN", model="activos.Activo").latest("timestamp")
        self.assertEqual(registro.object_id, str(self.activo_payan.pk))
        self.assertEqual(registro.payload, {"qr": True})
