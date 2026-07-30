import base64
import json
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.urls import reverse

from activos.models import Activo
from core.access import ACCESS_MANAGE
from core.models import Sucursal, UserModuleAccess, UserProfile
from fallas.models import CategoriaFalla, ReporteFalla
from operacion.models import RegistroHigiene, RespuestaHigiene


User = get_user_model()
PNG_1PX = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


@override_settings(SECURE_SSL_REDIRECT=False)
class HigieneDiariaTests(TestCase):
    def setUp(self):
        self.payan = Sucursal.objects.create(codigo="PAYAN-H", nombre="Payán")
        self.leyva = Sucursal.objects.create(codigo="LEYVA-H", nombre="Leyva")
        self.operadora = User.objects.create_user(username="higiene.payan", password="test12345")
        UserProfile.objects.create(user=self.operadora, sucursal=self.payan)
        UserModuleAccess.objects.create(user=self.operadora, module="fallas", access=ACCESS_MANAGE)
        self.otra_operadora = User.objects.create_user(username="higiene.leyva", password="test12345")
        UserProfile.objects.create(user=self.otra_operadora, sucursal=self.leyva)
        UserModuleAccess.objects.create(user=self.otra_operadora, module="fallas", access=ACCESS_MANAGE)
        self.supervisora = User.objects.create_user(username="comercial.higiene", password="test12345")
        UserProfile.objects.create(user=self.supervisora)
        UserModuleAccess.objects.create(
            user=self.supervisora, module="ventas.visitas_sucursal", access=ACCESS_MANAGE
        )
        self.categoria_instalacion = CategoriaFalla.objects.create(
            nombre="Plomería higiene", tipo=CategoriaFalla.TIPO_INSTALACION
        )
        self.categoria_equipo = CategoriaFalla.objects.create(
            nombre="Refrigeración higiene", tipo=CategoriaFalla.TIPO_EQUIPO
        )
        self.refrigerador = Activo.objects.create(nombre="Refrigerador mostrador", sucursal=self.payan)

    def _foto(self, name="evidencia.png"):
        return SimpleUploadedFile(name, PNG_1PX, content_type="image/png")

    def _guardar(self, *, tipo, clave_instancia, respuestas, archivos=None, **extra):
        data = {
            "tipo": tipo,
            "clave_instancia": clave_instancia,
            "hora": "09:30",
            "respuestas": json.dumps(respuestas),
            **extra,
        }
        data.update(archivos or {})
        return self.client.post(
            reverse("operacion:higiene_guardar"),
            data,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

    def test_app_muestra_higiene_a_sucursal_y_catalogo_versionado(self):
        self.client.force_login(self.operadora)

        home = self.client.get(reverse("operacion:app_home"))
        captura = self.client.get(reverse("operacion:higiene_home"))

        self.assertContains(home, "Higiene y limpieza")
        self.assertContains(home, reverse("operacion:higiene_home"))
        self.assertEqual(captura.status_code, 200)
        self.assertContains(captura, "Niveles de cloro y pH")
        self.assertContains(captura, "Programa de limpieza")
        self.assertContains(captura, "Limpieza de baños")
        self.assertContains(captura, "Vitrinas refrigeradas")
        self.assertContains(captura, "Jabón para manos")
        self.assertContains(captura, "Historial de Payán")
        self.assertContains(captura, 'data-capture-overview')
        self.assertContains(captura, 'data-section-step')
        self.assertContains(captura, 'data-section-next')
        self.assertContains(captura, 'data-progress-bar')
        self.assertContains(captura, "Revisión paso a paso")
        self.assertContains(captura, "Continuar")

    def test_cloro_y_ph_se_guardan_estructurados_y_sin_duplicar_el_dia(self):
        self.client.force_login(self.operadora)
        respuestas = [
            {"key": "cloro", "valor_numerico": "1.5"},
            {"key": "ph", "valor_numerico": "7.2"},
        ]

        primera = self._guardar(tipo="CLORO_PH", clave_instancia="red-principal", respuestas=respuestas)
        segunda = self._guardar(tipo="CLORO_PH", clave_instancia="red-principal", respuestas=respuestas)

        self.assertEqual(primera.status_code, 201)
        self.assertEqual(segunda.status_code, 200)
        self.assertEqual(RegistroHigiene.objects.count(), 1)
        registro = RegistroHigiene.objects.get()
        self.assertEqual(registro.plantilla_version, "2026.1")
        self.assertEqual(
            dict(registro.respuestas.values_list("punto_clave", "valor_numerico")),
            {"cloro": Decimal("1.50"), "ph": Decimal("7.20")},
        )

    def test_no_cumple_corregido_en_momento_no_genera_reporte_falla(self):
        self.client.force_login(self.operadora)

        response = self._guardar(
            tipo="LIMPIEZA",
            clave_instancia="diaria",
            respuestas=[
                {
                    "key": "mostrador_repisas",
                    "respuesta": "NO_CUMPLE",
                    "observacion": "Se limpió durante la revisión",
                    "corregido": True,
                    "requiere_seguimiento": False,
                }
            ],
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(RespuestaHigiene.objects.get().respuesta, "NO_CUMPLE")
        self.assertTrue(RespuestaHigiene.objects.get().corregido_en_momento)
        self.assertFalse(ReporteFalla.objects.exists())

    def test_evidencia_de_higiene_crea_una_sola_falla_y_reutiliza_archivo(self):
        self.client.force_login(self.operadora)
        respuesta = {
            "key": "mostrador_agua_refrigeradores",
            "respuesta": "NO_CUMPLE",
            "observacion": "Hay fuga y acumulación bajo la vitrina",
            "corregido": False,
            "requiere_seguimiento": True,
            "tipo_objetivo": "INSTALACION",
            "categoria_id": self.categoria_instalacion.id,
            "area_instalacion": "Mostrador",
            "prioridad": "alta",
        }

        primera = self._guardar(
            tipo="LIMPIEZA",
            clave_instancia="diaria",
            respuestas=[respuesta],
            archivos={"evidencia_mostrador_agua_refrigeradores": self._foto()},
        )
        segunda = self._guardar(
            tipo="LIMPIEZA",
            clave_instancia="diaria",
            respuestas=[respuesta],
        )
        tercera = self._guardar(
            tipo="LIMPIEZA",
            clave_instancia="diaria",
            respuestas=[
                {
                    "key": "mostrador_agua_refrigeradores",
                    "respuesta": "NO_CUMPLE",
                    "observacion": "Intento de quitar seguimiento",
                    "corregido": True,
                    "requiere_seguimiento": False,
                }
            ],
        )

        self.assertEqual(primera.status_code, 201)
        self.assertEqual(segunda.status_code, 200)
        self.assertEqual(tercera.status_code, 200)
        self.assertEqual(ReporteFalla.objects.count(), 1)
        revision = RespuestaHigiene.objects.get()
        reporte = revision.reporte_falla
        self.assertTrue(revision.requiere_seguimiento)
        self.assertFalse(revision.corregido_en_momento)
        self.assertEqual(reporte.sucursal, self.payan)
        self.assertEqual(reporte.reportado_por, self.operadora)
        self.assertEqual(reporte.area_instalacion, "Mostrador")
        self.assertEqual(reporte.foto_evidencia.name, revision.evidencia.name)
        self.assertIn("Programa de limpieza", reporte.titulo)
        self.assertEqual(primera.json()["reporte_falla_ids"], [reporte.id])
        self.assertEqual(segunda.json()["reporte_falla_ids"], [reporte.id])

    def test_falla_de_equipo_solo_admite_activo_de_la_sucursal(self):
        activo_ajeno = Activo.objects.create(nombre="Equipo Leyva", sucursal=self.leyva)
        self.client.force_login(self.operadora)
        base = {
            "key": "produccion_equipos_limpios",
            "respuesta": "NO_CUMPLE",
            "observacion": "No enfría",
            "requiere_seguimiento": True,
            "tipo_objetivo": "EQUIPO",
            "categoria_id": self.categoria_equipo.id,
            "prioridad": "media",
        }

        rechazada = self._guardar(
            tipo="LIMPIEZA",
            clave_instancia="diaria",
            respuestas=[{**base, "activo_id": activo_ajeno.id}],
            archivos={"evidencia_produccion_equipos_limpios": self._foto("ajena.png")},
        )

        self.assertEqual(rechazada.status_code, 400)
        self.assertFalse(RegistroHigiene.objects.exists())
        self.assertFalse(ReporteFalla.objects.exists())

    def test_sucursal_solo_ve_su_historial_y_supervision_puede_filtrar_e_imprimir(self):
        RegistroHigiene.objects.create(
            tipo="BANOS", sucursal=self.payan, fecha="2026-07-30", clave_instancia="clientes-ronda-1",
            plantilla_version="2026.1", creado_por=self.operadora,
        )
        RegistroHigiene.objects.create(
            tipo="BANOS", sucursal=self.leyva, fecha="2026-07-30", clave_instancia="personal-ronda-1",
            plantilla_version="2026.1", creado_por=self.otra_operadora,
        )

        self.client.force_login(self.operadora)
        propio = self.client.get(reverse("operacion:higiene_historial"))
        intento_ajeno = self.client.get(
            reverse("operacion:higiene_historial"), {"sucursal": self.leyva.id}
        )
        self.assertContains(propio, "clientes-ronda-1")
        self.assertNotContains(propio, "personal-ronda-1")
        self.assertContains(intento_ajeno, "clientes-ronda-1")
        self.assertNotContains(intento_ajeno, "personal-ronda-1")

        self.client.force_login(self.supervisora)
        global_history = self.client.get(reverse("operacion:higiene_historial"))
        printable = self.client.get(
            reverse("operacion:higiene_imprimir"), {"sucursal": self.leyva.id, "tipo": "BANOS"}
        )
        self.assertContains(global_history, "clientes-ronda-1")
        self.assertContains(global_history, "personal-ronda-1")
        self.assertContains(printable, "Reporte de higiene y limpieza")
        self.assertContains(printable, "Leyva")
        self.assertNotContains(printable, "Payán")
        self.assertContains(printable, "window.print")

    def test_usuario_sin_sucursal_ni_supervision_no_puede_abrir_rutas_directas(self):
        usuario = User.objects.create_user(username="sin.higiene", password="test12345")
        UserProfile.objects.create(user=usuario)
        self.client.force_login(usuario)

        for name in (
            "operacion:higiene_home",
            "operacion:higiene_historial",
            "operacion:higiene_imprimir",
        ):
            with self.subTest(name=name):
                self.assertEqual(self.client.get(reverse(name)).status_code, 403)
