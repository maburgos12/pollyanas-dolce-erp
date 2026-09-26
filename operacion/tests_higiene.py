import base64
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
from pathlib import Path
from threading import Barrier
from tempfile import TemporaryDirectory
from unittest import mock

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import close_old_connections, connections
from django.test import Client, TestCase, TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from activos.models import Activo
from core.access import ACCESS_MANAGE
from core.models import Sucursal, UserModuleAccess, UserProfile
from fallas.models import CategoriaFalla, ReporteFalla
from operacion.models import RegistroHigiene, RespuestaHigiene
from operacion.services_fallas import notificar_evento_higiene


User = get_user_model()
PNG_1PX = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


@override_settings(SECURE_SSL_REDIRECT=False)
class HigieneDiariaTests(TestCase):
    def setUp(self):
        self.media_tmp = TemporaryDirectory()
        self.media_override = override_settings(MEDIA_ROOT=self.media_tmp.name)
        self.media_override.enable()
        self.addCleanup(self.media_tmp.cleanup)
        self.addCleanup(self.media_override.disable)
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

    def _hallazgo_banos(
        self,
        *,
        observacion="El sanitario no descarga agua",
        decision="AUTO",
        reporte_id=None,
        key="bano_sanitario",
    ):
        hallazgo = {
            "key": key,
            "respuesta": "NO_CUMPLE",
            "observacion": observacion,
            "corregido": False,
            "requiere_seguimiento": True,
            "tipo_objetivo": "INSTALACION",
            "categoria_id": self.categoria_instalacion.id,
            "area_instalacion": "Baños",
            "prioridad": "alta",
            "falla_decision": decision,
        }
        if reporte_id is not None:
            hallazgo["reporte_falla_id"] = reporte_id
        return hallazgo

    def _crear_falla_higiene_abierta(self, *, fecha=date(2026, 9, 25)):
        with mock.patch("operacion.services_higiene.timezone.localdate", return_value=fecha):
            response = self._guardar(
                tipo="BANOS",
                clave_instancia=f"clientes-{fecha.isoformat()}",
                respuestas=[self._hallazgo_banos()],
                archivos={"evidencia_bano_sanitario": self._foto(f"sanitario-{fecha}.png")},
            )
        self.assertEqual(response.status_code, 201)
        return ReporteFalla.objects.get()

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
        self.assertContains(captura, 'data-icon-source="lucide"')
        self.assertContains(captura, 'id="higiene-icon-beaker"')
        self.assertContains(captura, 'id="higiene-icon-spray-can"')
        self.assertContains(captura, 'id="higiene-icon-toilet"')
        self.assertContains(captura, 'href="#higiene-icon-beaker"')
        self.assertContains(captura, 'href="#higiene-icon-spray-can"')
        self.assertContains(captura, 'href="#higiene-icon-toilet"')
        self.assertNotContains(captura, ">H₂O<")
        self.assertNotContains(captura, ">WC<")

    def test_usuario_solo_mermas_tambien_puede_abrir_higiene(self):
        solo_mermas = User.objects.create_user(username="mermas.higiene", password="test12345")
        UserProfile.objects.create(user=solo_mermas, sucursal=self.payan)
        UserModuleAccess.objects.create(
            user=solo_mermas,
            module="mermas.captura",
            access=ACCESS_MANAGE,
        )
        self.client.force_login(solo_mermas)

        home = self.client.get(reverse("operacion:app_home"))
        captura = self.client.get(reverse("operacion:higiene_home"))

        self.assertContains(home, "Higiene y limpieza")
        self.assertEqual(captura.status_code, 200)
        self.assertContains(captura, "Historial de Payán")

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

    def test_varias_revisiones_pueden_apuntar_a_la_misma_falla(self):
        reporte = ReporteFalla.objects.create(
            sucursal=self.payan,
            categoria=self.categoria_instalacion,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            titulo="Sanitario sin funcionar",
            descripcion="No descarga agua.",
            justificacion_sin_foto="Prueba automatizada.",
            reportado_por=self.operadora,
        )
        for fecha in ("2026-09-25", "2026-09-26"):
            registro = RegistroHigiene.objects.create(
                tipo=RegistroHigiene.TIPO_BANOS,
                sucursal=self.payan,
                fecha=fecha,
                clave_instancia=f"clientes-{fecha}",
                plantilla_version="2026.1",
                creado_por=self.operadora,
            )
            RespuestaHigiene.objects.create(
                registro=registro,
                punto_clave="banos_sanitario",
                seccion="Limpieza de baños",
                punto_revision="Sanitario limpio y funcional",
                respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
                requiere_seguimiento=True,
                tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
                area_instalacion="Baños",
                reporte_falla=reporte,
                continuidad_falla=RespuestaHigiene.CONTINUIDAD_IGUAL,
            )

        self.assertEqual(reporte.constataciones_higiene.count(), 2)

    def test_sigue_igual_en_otro_dia_reutiliza_la_falla(self):
        self.client.force_login(self.operadora)
        principal = self._crear_falla_higiene_abierta()

        with mock.patch(
            "operacion.services_higiene.timezone.localdate", return_value=date(2026, 9, 26)
        ):
            response = self._guardar(
                tipo="BANOS",
                clave_instancia="clientes-2026-09-26",
                respuestas=[
                    self._hallazgo_banos(decision="MISMA", reporte_id=principal.pk)
                ],
            )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(ReporteFalla.objects.count(), 1)
        self.assertEqual(principal.constataciones_higiene.count(), 2)

    def test_problema_distinto_crea_otro_reporte_aunque_coincida_el_punto(self):
        self.client.force_login(self.operadora)
        principal = self._crear_falla_higiene_abierta()

        with mock.patch(
            "operacion.services_higiene.timezone.localdate", return_value=date(2026, 9, 26)
        ):
            response = self._guardar(
                tipo="BANOS",
                clave_instancia="clientes-2026-09-26",
                respuestas=[
                    self._hallazgo_banos(
                        observacion="Ahora también se desprendió la puerta",
                        decision="DISTINTA",
                        reporte_id=principal.pk,
                    )
                ],
                archivos={"evidencia_bano_sanitario": self._foto("puerta.png")},
            )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(ReporteFalla.objects.count(), 2)

    def test_reporte_cerrado_no_acepta_continuidad_y_se_convierte_en_reincidencia(self):
        self.client.force_login(self.operadora)
        principal = self._crear_falla_higiene_abierta()
        principal.estatus = ReporteFalla.ESTATUS_CERRADO
        principal.fecha_cierre = timezone.now()
        principal.save(update_fields=["estatus", "fecha_cierre"])

        with mock.patch(
            "operacion.services_higiene.timezone.localdate", return_value=date(2026, 9, 26)
        ):
            response = self._guardar(
                tipo="BANOS",
                clave_instancia="clientes-2026-09-26",
                respuestas=[
                    self._hallazgo_banos(decision="MISMA", reporte_id=principal.pk)
                ],
                archivos={"evidencia_bano_sanitario": self._foto("reincidencia.png")},
            )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(ReporteFalla.objects.count(), 2)

    def test_reporte_cerrado_sin_evidencia_rechaza_y_revierte_la_captura(self):
        self.client.force_login(self.operadora)
        principal = self._crear_falla_higiene_abierta()
        principal.estatus = ReporteFalla.ESTATUS_CERRADO
        principal.fecha_cierre = timezone.now()
        principal.save(update_fields=["estatus", "fecha_cierre"])

        with mock.patch(
            "operacion.services_higiene.timezone.localdate", return_value=date(2026, 9, 26)
        ):
            response = self._guardar(
                tipo="BANOS",
                clave_instancia="clientes-2026-09-26",
                respuestas=[
                    self._hallazgo_banos(decision="MISMA", reporte_id=principal.pk)
                ],
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(ReporteFalla.objects.count(), 1)
        self.assertEqual(RegistroHigiene.objects.count(), 1)
        self.assertEqual(RespuestaHigiene.objects.count(), 1)

    def test_cambio_y_correccion_enlazan_la_falla_activa(self):
        self.client.force_login(self.operadora)
        principal = self._crear_falla_higiene_abierta()

        for fecha, decision in (
            (date(2026, 9, 26), "CAMBIO"),
            (date(2026, 9, 27), "CORRECCION_PENDIENTE"),
        ):
            with mock.patch(
                "operacion.services_higiene.timezone.localdate", return_value=fecha
            ):
                response = self._guardar(
                    tipo="BANOS",
                    clave_instancia=f"clientes-{fecha}",
                    respuestas=[
                        self._hallazgo_banos(
                            observacion=f"Constatación {decision}",
                            decision=decision,
                            reporte_id=principal.pk,
                        )
                    ],
                    archivos={
                        "evidencia_bano_sanitario": self._foto(f"{decision}.png")
                    },
                )
            self.assertEqual(response.status_code, 201)

        self.assertEqual(ReporteFalla.objects.count(), 1)
        self.assertEqual(
            list(
                principal.constataciones_higiene.order_by("registro__fecha").values_list(
                    "continuidad_falla", flat=True
                )
            ),
            [
                RespuestaHigiene.CONTINUIDAD_INICIAL,
                RespuestaHigiene.CONTINUIDAD_CAMBIO,
                RespuestaHigiene.CONTINUIDAD_CORRECCION,
            ],
        )

    def test_auto_con_coincidencia_responde_409_y_revierte_el_registro(self):
        with TemporaryDirectory() as media_root, self.settings(MEDIA_ROOT=media_root):
            self.client.force_login(self.operadora)
            principal = self._crear_falla_higiene_abierta()
            archivos_antes = {
                path.relative_to(media_root)
                for path in Path(media_root).rglob("*")
                if path.is_file()
            }

            with mock.patch(
                "operacion.services_higiene.timezone.localdate", return_value=date(2026, 9, 26)
            ):
                response = self._guardar(
                    tipo="BANOS",
                    clave_instancia="clientes-2026-09-26",
                    respuestas=[self._hallazgo_banos()],
                    archivos={"evidencia_bano_sanitario": self._foto("duplicada.png")},
                )
            archivos_despues = {
                path.relative_to(media_root)
                for path in Path(media_root).rglob("*")
                if path.is_file()
            }

            self.assertEqual(response.status_code, 409)
            self.assertEqual(response.json()["existing_reports"][0]["id"], principal.pk)
            self.assertEqual(RegistroHigiene.objects.count(), 1)
            self.assertEqual(RespuestaHigiene.objects.count(), 1)
            self.assertEqual(archivos_despues, archivos_antes)

    def test_misma_activa_de_otro_punto_rechaza_sin_degradar_a_auto(self):
        with TemporaryDirectory() as media_root, self.settings(MEDIA_ROOT=media_root):
            self.client.force_login(self.operadora)
            principal = self._crear_falla_higiene_abierta()
            archivos_antes = set(Path(media_root).rglob("*.png"))

            with mock.patch(
                "operacion.services_higiene.timezone.localdate", return_value=date(2026, 9, 26)
            ):
                response = self._guardar(
                    tipo="BANOS",
                    clave_instancia="clientes-otro-punto",
                    respuestas=[
                        self._hallazgo_banos(
                            key="bano_pisos",
                            decision="MISMA",
                            reporte_id=principal.pk,
                        )
                    ],
                    archivos={"evidencia_bano_pisos": self._foto("otro-punto.png")},
                )

            self.assertEqual(response.status_code, 400)
            self.assertEqual(ReporteFalla.objects.count(), 1)
            self.assertEqual(RegistroHigiene.objects.count(), 1)
            self.assertEqual(RespuestaHigiene.objects.count(), 1)
            self.assertEqual(set(Path(media_root).rglob("*.png")), archivos_antes)

    def test_reporte_cerrado_de_otro_punto_no_crea_reincidencia(self):
        with TemporaryDirectory() as media_root, self.settings(MEDIA_ROOT=media_root):
            self.client.force_login(self.operadora)
            principal = self._crear_falla_higiene_abierta()
            principal.estatus = ReporteFalla.ESTATUS_CERRADO
            principal.fecha_cierre = timezone.now()
            principal.save(update_fields=["estatus", "fecha_cierre"])
            archivos_antes = set(Path(media_root).rglob("*.png"))

            with mock.patch(
                "operacion.services_higiene.timezone.localdate", return_value=date(2026, 9, 26)
            ):
                response = self._guardar(
                    tipo="BANOS",
                    clave_instancia="clientes-cerrado-otro-punto",
                    respuestas=[
                        self._hallazgo_banos(
                            key="bano_pisos",
                            decision="MISMA",
                            reporte_id=principal.pk,
                        )
                    ],
                    archivos={"evidencia_bano_pisos": self._foto("cerrado-otro-punto.png")},
                )

            self.assertEqual(response.status_code, 400)
            self.assertEqual(ReporteFalla.objects.count(), 1)
            self.assertEqual(RegistroHigiene.objects.count(), 1)
            self.assertEqual(RespuestaHigiene.objects.count(), 1)
            self.assertEqual(set(Path(media_root).rglob("*.png")), archivos_antes)

    def _assert_reporte_falla_id_invalido(self, valor):
        self.client.force_login(self.operadora)
        principal = self._crear_falla_higiene_abierta()
        with mock.patch(
            "operacion.services_higiene.timezone.localdate", return_value=date(2026, 9, 26)
        ):
            response = self._guardar(
                tipo="BANOS",
                clave_instancia=f"id-invalido-{type(valor).__name__}",
                respuestas=[
                    self._hallazgo_banos(decision="MISMA", reporte_id=valor)
                ],
            )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(ReporteFalla.objects.count(), 1)
        self.assertEqual(RegistroHigiene.objects.count(), 1)
        self.assertEqual(RespuestaHigiene.objects.count(), 1)
        self.assertEqual(principal.constataciones_higiene.count(), 1)

    def test_reporte_falla_id_booleano_es_invalido(self):
        self._assert_reporte_falla_id_invalido(True)

    def test_reporte_falla_id_float_es_invalido(self):
        self._assert_reporte_falla_id_invalido(1.9)

    def test_reporte_falla_id_decimal_en_texto_es_invalido(self):
        self._assert_reporte_falla_id_invalido("1.9")

    def test_constatacion_difiere_notificacion_hasta_commit(self):
        self.client.force_login(self.operadora)
        principal = self._crear_falla_higiene_abierta()

        with mock.patch(
            "operacion.services_fallas.notificar_evento_higiene"
        ) as notificar:
            with self.captureOnCommitCallbacks(execute=True):
                with mock.patch(
                    "operacion.services_higiene.timezone.localdate",
                    return_value=date(2026, 9, 26),
                ):
                    response = self._guardar(
                        tipo="BANOS",
                        clave_instancia="clientes-2026-09-26",
                        respuestas=[
                            self._hallazgo_banos(decision="MISMA", reporte_id=principal.pk)
                        ],
                    )
                self.assertEqual(response.status_code, 201)
                notificar.assert_not_called()
            notificar.assert_called_once()

    def test_sigue_igual_no_avisa_diario_solo_en_dias_tres_y_seis(self):
        self.client.force_login(self.operadora)
        reporte = self._crear_falla_higiene_abierta()
        reporte.fecha_reporte = timezone.make_aware(
            timezone.datetime(2026, 9, 25, 9, 0)
        )
        reporte.save(update_fields=["fecha_reporte"])

        with mock.patch("operacion.services_fallas._usuarios_mantenimiento", return_value=[]), mock.patch(
            "operacion.services_fallas.crear_notificaciones"
        ) as crear:
            for fecha in (date(2026, 9, 26), date(2026, 9, 28), date(2026, 10, 1)):
                registro = RegistroHigiene.objects.create(
                    tipo=RegistroHigiene.TIPO_BANOS,
                    sucursal=self.payan,
                    fecha=fecha,
                    clave_instancia=f"aviso-{fecha}",
                    plantilla_version="2026.1",
                    creado_por=self.operadora,
                )
                respuesta = RespuestaHigiene.objects.create(
                    registro=registro,
                    punto_clave="bano_sanitario",
                    seccion="Interior",
                    punto_revision="Sanitario limpio y funcional",
                    respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
                    observacion="Sigue sin descargar",
                    requiere_seguimiento=True,
                    reporte_falla=reporte,
                    continuidad_falla=RespuestaHigiene.CONTINUIDAD_IGUAL,
                )
                notificar_evento_higiene(reporte, respuesta, self.operadora)

        self.assertEqual(crear.call_count, 2)
        self.assertEqual(
            [llamada.kwargs["titulo"] for llamada in crear.call_args_list],
            [
                "Falla sin resolver por 3 días en Payán",
                "Falla sin resolver por 6 días en Payán",
            ],
        )

    def test_cambio_y_correccion_generan_avisos_especificos(self):
        self.client.force_login(self.operadora)
        reporte = self._crear_falla_higiene_abierta()
        registro = RegistroHigiene.objects.create(
            tipo=RegistroHigiene.TIPO_BANOS,
            sucursal=self.payan,
            fecha=date(2026, 9, 26),
            clave_instancia="avisos-decision",
            plantilla_version="2026.1",
            creado_por=self.operadora,
        )

        with mock.patch("operacion.services_fallas._usuarios_mantenimiento", return_value=[]), mock.patch(
            "operacion.services_fallas.crear_notificaciones"
        ) as crear:
            for continuidad in (
                RespuestaHigiene.CONTINUIDAD_CAMBIO,
                RespuestaHigiene.CONTINUIDAD_CORRECCION,
            ):
                respuesta = RespuestaHigiene.objects.create(
                    registro=registro,
                    punto_clave=f"bano_sanitario_{continuidad}",
                    seccion="Interior",
                    punto_revision="Sanitario limpio y funcional",
                    respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
                    observacion="Revisión del estado",
                    requiere_seguimiento=True,
                    reporte_falla=reporte,
                    continuidad_falla=continuidad,
                )
                notificar_evento_higiene(reporte, respuesta, self.operadora)

        self.assertEqual(
            [llamada.kwargs["titulo"] for llamada in crear.call_args_list],
            [
                "Falla cambió o empeoró en Payán",
                "Validar corrección en Payán",
            ],
        )

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
        self.assertContains(global_history, 'href="#higiene-icon-printer"')
        self.assertContains(global_history, 'href="#higiene-icon-arrow-left"')
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


@override_settings(SECURE_SSL_REDIRECT=False)
class HigieneConcurrenciaTests(TransactionTestCase):
    def setUp(self):
        self.media_tmp = TemporaryDirectory()
        self.media_override = override_settings(MEDIA_ROOT=self.media_tmp.name)
        self.media_override.enable()
        self.addCleanup(self.media_tmp.cleanup)
        self.addCleanup(self.media_override.disable)
        self.sucursal = Sucursal.objects.create(codigo="CONC-H", nombre="Concurrencia")
        self.categoria = CategoriaFalla.objects.create(
            nombre="Plomería concurrente",
            tipo=CategoriaFalla.TIPO_INSTALACION,
        )
        self.usuarios = []
        for numero in (1, 2):
            usuario = User.objects.create_user(
                username=f"higiene.concurrente.{numero}",
                password="test12345",
            )
            UserProfile.objects.create(user=usuario, sucursal=self.sucursal)
            UserModuleAccess.objects.create(
                user=usuario,
                module="fallas",
                access=ACCESS_MANAGE,
            )
            self.usuarios.append(usuario)

    def _hallazgo(self, *, decision="AUTO", reporte_id=None):
        hallazgo = {
            "key": "bano_sanitario",
            "respuesta": "NO_CUMPLE",
            "observacion": "Sanitario sin descarga",
            "corregido": False,
            "requiere_seguimiento": True,
            "tipo_objetivo": "INSTALACION",
            "categoria_id": self.categoria.pk,
            "area_instalacion": "Baños",
            "prioridad": "alta",
            "falla_decision": decision,
        }
        if reporte_id is not None:
            hallazgo["reporte_falla_id"] = reporte_id
        return hallazgo

    def _post_concurrente(self, *, usuario_id, instancia, barrier, hallazgo, con_foto):
        close_old_connections()
        try:
            cliente = Client()
            cliente.force_login(User.objects.get(pk=usuario_id))
            data = {
                "tipo": "BANOS",
                "clave_instancia": instancia,
                "hora": "09:30",
                "respuestas": json.dumps([hallazgo]),
            }
            if con_foto:
                data["evidencia_bano_sanitario"] = SimpleUploadedFile(
                    f"{instancia}.png",
                    PNG_1PX,
                    content_type="image/png",
                )
            barrier.wait()
            return cliente.post(
                reverse("operacion:higiene_guardar"),
                data,
                HTTP_X_REQUESTED_WITH="XMLHttpRequest",
            ).status_code
        finally:
            connections.close_all()

    def test_dos_auto_simultaneos_crean_un_principal(self):
        barrier = Barrier(2)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futuros = [
                executor.submit(
                    self._post_concurrente,
                    usuario_id=usuario.pk,
                    instancia=f"auto-{indice}",
                    barrier=barrier,
                    hallazgo=self._hallazgo(),
                    con_foto=True,
                )
                for indice, usuario in enumerate(self.usuarios, start=1)
            ]
            statuses = sorted(futuro.result() for futuro in futuros)

        self.assertEqual(statuses, [201, 409])
        self.assertEqual(ReporteFalla.objects.filter(duplicado_de__isnull=True).count(), 1)
        self.assertEqual(len(list(Path(self.media_tmp.name).rglob("*.png"))), 1)

    def test_dos_misma_simultaneos_reutilizan_el_principal(self):
        cliente = Client()
        cliente.force_login(self.usuarios[0])
        principal_response = cliente.post(
            reverse("operacion:higiene_guardar"),
            {
                "tipo": "BANOS",
                "clave_instancia": "principal",
                "hora": "09:30",
                "respuestas": json.dumps([self._hallazgo()]),
                "evidencia_bano_sanitario": SimpleUploadedFile(
                    "principal.png", PNG_1PX, content_type="image/png"
                ),
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(principal_response.status_code, 201)
        principal = ReporteFalla.objects.get()

        barrier = Barrier(2)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futuros = [
                executor.submit(
                    self._post_concurrente,
                    usuario_id=usuario.pk,
                    instancia=f"misma-{indice}",
                    barrier=barrier,
                    hallazgo=self._hallazgo(decision="MISMA", reporte_id=principal.pk),
                    con_foto=False,
                )
                for indice, usuario in enumerate(self.usuarios, start=1)
            ]
            statuses = sorted(futuro.result() for futuro in futuros)

        self.assertEqual(statuses, [201, 201])
        self.assertEqual(principal.constataciones_higiene.count(), 3)
