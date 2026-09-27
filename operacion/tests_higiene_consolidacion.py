from datetime import date, datetime
from tempfile import TemporaryDirectory

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.utils import timezone

from core.models import Notificacion, Sucursal
from fallas.models import BitacoraFalla, CategoriaFalla, ReporteFalla
from operacion.models import RegistroHigiene, RespuestaHigiene
from operacion.services_higiene_consolidacion import proponer_consolidacion_higiene


class ConsolidacionHigieneTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._media_dir = TemporaryDirectory()
        cls._media_override = override_settings(MEDIA_ROOT=cls._media_dir.name)
        cls._media_override.enable()

    @classmethod
    def tearDownClass(cls):
        cls._media_override.disable()
        cls._media_dir.cleanup()
        super().tearDownClass()

    def setUp(self):
        users = get_user_model()
        self.dg = users.objects.create_user(username="dg.consolidacion", is_superuser=True)
        self.operadora = users.objects.create_user(username="higiene.consolidacion")
        self.sucursal = Sucursal.objects.create(
            codigo="HIG-CONS",
            nombre="Sucursal consolidación",
            activa=True,
        )
        self.categoria = CategoriaFalla.objects.create(
            nombre="Plomería consolidación",
            tipo=CategoriaFalla.TIPO_INSTALACION,
        )

    def _crear_reporte_respuesta(self, *, fecha, observacion, indice):
        reporte = ReporteFalla.objects.create(
            sucursal=self.sucursal,
            categoria=self.categoria,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            titulo="Limpieza de baños · Sanitario limpio y funcional",
            descripcion=f"Hallazgo de higiene: {observacion}",
            justificacion_sin_foto="Prueba automatizada.",
            reportado_por=self.operadora,
            fecha_reporte=timezone.make_aware(datetime.combine(fecha, datetime.min.time())),
        )
        registro = RegistroHigiene.objects.create(
            tipo=RegistroHigiene.TIPO_BANOS,
            sucursal=self.sucursal,
            fecha=fecha,
            clave_instancia=f"clientes-ronda-{indice}",
            plantilla_version="2026.1",
            creado_por=self.operadora,
        )
        respuesta = RespuestaHigiene.objects.create(
            registro=registro,
            punto_clave="banos_sanitario",
            seccion="Limpieza de baños",
            punto_revision="Sanitario limpio y funcional",
            respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
            observacion=observacion,
            evidencia=SimpleUploadedFile(
                f"evidencia-{indice}.png",
                b"\x89PNG\r\n\x1a\nprueba",
                content_type="image/png",
            ),
            requiere_seguimiento=True,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            reporte_falla=reporte,
            continuidad_falla=RespuestaHigiene.CONTINUIDAD_INICIAL,
        )
        return reporte, respuesta

    def crear_repeticiones(self, *, observaciones):
        principal, respuesta_principal = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion=observaciones[0],
            indice=1,
        )
        repetido, respuesta_repetida = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 26),
            observacion=observaciones[1],
            indice=2,
        )
        return principal, repetido, respuesta_principal, respuesta_repetida

    def crear_repeticiones_con_cierre_intermedio(self):
        principal, posterior, _, _ = self.crear_repeticiones(
            observaciones=("No descarga agua", "No descarga agua"),
        )
        principal.estatus = ReporteFalla.ESTATUS_CERRADO
        principal.fecha_cierre = timezone.make_aware(datetime(2026, 9, 25, 18, 0))
        principal.save(update_fields=["estatus", "fecha_cierre"])
        return principal, posterior

    def test_preview_agrupa_identidad_y_observacion_exactas_sin_escribir(self):
        principal, repetido, respuesta_principal, respuesta_repetida = self.crear_repeticiones(
            observaciones=("  No descarga  água ", "no descarga agua"),
        )

        propuestas = proponer_consolidacion_higiene()

        self.assertEqual(len(propuestas.exactas), 1)
        propuesta = propuestas.exactas[0]
        self.assertEqual(propuesta.principal_id, principal.id)
        self.assertEqual(propuesta.repetido_id, repetido.id)
        self.assertEqual(propuesta.respuesta_principal_id, respuesta_principal.id)
        self.assertEqual(propuesta.respuesta_repetida_id, respuesta_repetida.id)
        self.assertTrue(propuesta.exacta)
        repetido.refresh_from_db()
        self.assertIsNone(repetido.duplicado_de_id)

    def test_preview_deja_observaciones_distintas_como_ambiguas(self):
        principal, repetido, _, _ = self.crear_repeticiones(
            observaciones=("No descarga agua", "La tapa está rota"),
        )

        propuestas = proponer_consolidacion_higiene()

        self.assertEqual(len(propuestas.ambiguas), 1)
        self.assertEqual(propuestas.ambiguas[0].principal_id, principal.id)
        self.assertEqual(propuestas.ambiguas[0].repetido_id, repetido.id)
        self.assertFalse(propuestas.ambiguas[0].exacta)

    def test_reaparicion_despues_del_cierre_inicia_otro_ciclo(self):
        principal, posterior = self.crear_repeticiones_con_cierre_intermedio()

        propuestas = proponer_consolidacion_higiene()

        self.assertNotIn(
            (principal.id, posterior.id),
            {(row.principal_id, row.repetido_id) for row in propuestas.todas},
        )

    def test_dos_previews_no_cambian_registros_vinculos_bitacoras_ni_notificaciones(self):
        principal, repetido, respuesta_principal, respuesta_repetida = self.crear_repeticiones(
            observaciones=("No descarga agua", "No descarga agua"),
        )
        BitacoraFalla.objects.create(
            reporte=principal,
            usuario=self.dg,
            estatus_anterior="",
            estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
            comentario="Estado previo de prueba.",
        )
        Notificacion.objects.create(
            usuario=self.dg,
            actor=self.operadora,
            titulo="Notificación previa de prueba",
            objeto_tipo="falla",
            objeto_id=str(principal.id),
        )
        estado_inicial = self._estado_persistido()

        primera = proponer_consolidacion_higiene()
        segunda = proponer_consolidacion_higiene()

        self.assertEqual(primera, segunda)
        self.assertEqual(self._estado_persistido(), estado_inicial)
        self.assertEqual(
            set(RespuestaHigiene.objects.values_list("id", "reporte_falla_id")),
            {
                (respuesta_principal.id, principal.id),
                (respuesta_repetida.id, repetido.id),
            },
        )

    @staticmethod
    def _estado_persistido():
        return {
            "reportes": ReporteFalla.objects.count(),
            "respuestas": RespuestaHigiene.objects.count(),
            "registros": RegistroHigiene.objects.count(),
            "bitacoras": BitacoraFalla.objects.count(),
            "notificaciones": Notificacion.objects.count(),
            "duplicados": tuple(
                ReporteFalla.objects.order_by("id").values_list("id", "duplicado_de_id")
            ),
            "vinculos": tuple(
                RespuestaHigiene.objects.order_by("id").values_list(
                    "id",
                    "reporte_falla_id",
                    "continuidad_falla",
                )
            ),
        }
