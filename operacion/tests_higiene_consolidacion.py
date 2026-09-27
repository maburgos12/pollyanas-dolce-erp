from datetime import date, datetime
from tempfile import TemporaryDirectory
from threading import Barrier, Thread
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import close_old_connections, connection
from django.test import TestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from activos.models import Activo
from core.models import AuditLog, Notificacion, Sucursal
from fallas.models import BitacoraFalla, CategoriaFalla, ReporteFalla
from operacion.models import RegistroHigiene, RespuestaHigiene
from operacion.services_higiene_consolidacion import (
    aplicar_consolidacion_higiene,
    proponer_consolidacion_higiene,
)


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

    def _crear_reporte_respuesta(
        self,
        *,
        fecha,
        observacion,
        indice,
        fecha_reporte=None,
        categoria=None,
        tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
        activo=None,
        area_instalacion="Baños",
        respuesta_tipo_objetivo=None,
        respuesta_activo=None,
        respuesta_area_instalacion=None,
        reporte_sucursal=None,
        registro_sucursal=None,
    ):
        categoria = categoria or self.categoria
        reporte = ReporteFalla.objects.create(
            sucursal=reporte_sucursal or self.sucursal,
            categoria=categoria,
            tipo_objetivo=tipo_objetivo,
            activo_relacionado=activo,
            area_instalacion=area_instalacion,
            titulo="Limpieza de baños · Sanitario limpio y funcional",
            descripcion=f"Hallazgo de higiene: {observacion}",
            justificacion_sin_foto="Prueba automatizada.",
            reportado_por=self.operadora,
            fecha_reporte=fecha_reporte
            or timezone.make_aware(datetime.combine(fecha, datetime.min.time())),
        )
        registro = RegistroHigiene.objects.create(
            tipo=RegistroHigiene.TIPO_BANOS,
            sucursal=registro_sucursal or self.sucursal,
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
            tipo_objetivo=respuesta_tipo_objetivo or tipo_objetivo,
            activo_relacionado=respuesta_activo,
            area_instalacion=(
                area_instalacion
                if respuesta_area_instalacion is None
                else respuesta_area_instalacion
            ),
            reporte_falla=reporte,
            continuidad_falla=RespuestaHigiene.CONTINUIDAD_INICIAL,
        )
        return reporte, respuesta

    def _transicion(self, reporte, *, anterior, nuevo, cuando):
        return BitacoraFalla.objects.create(
            reporte=reporte,
            usuario=self.dg,
            estatus_anterior=anterior,
            estatus_nuevo=nuevo,
            comentario="Transición histórica de prueba.",
            timestamp=cuando,
        )

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

    def test_identidad_usa_clasificacion_del_reporte_aunque_la_respuesta_sea_inconsistente(self):
        principal, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion="No descarga agua",
            indice=1,
            respuesta_tipo_objetivo=ReporteFalla.OBJETIVO_EQUIPO,
            respuesta_area_instalacion="",
        )
        repetido, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 26),
            observacion="No descarga agua",
            indice=2,
            respuesta_tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            respuesta_area_instalacion="Área capturada incorrectamente",
        )

        propuestas = proponer_consolidacion_higiene()

        self.assertEqual(
            {(row.principal_id, row.repetido_id) for row in propuestas.exactas},
            {(principal.id, repetido.id)},
        )

    def test_identidad_no_agrupa_reportes_de_equipos_distintos(self):
        categoria_equipo = CategoriaFalla.objects.create(
            nombre="Equipo consolidación",
            tipo=CategoriaFalla.TIPO_EQUIPO,
        )
        activo_uno = Activo.objects.create(
            codigo="HIG-EQ-1",
            nombre="Equipo uno",
            sucursal=self.sucursal,
            creado_por=self.dg,
        )
        activo_dos = Activo.objects.create(
            codigo="HIG-EQ-2",
            nombre="Equipo dos",
            sucursal=self.sucursal,
            creado_por=self.dg,
        )
        principal, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion="No enciende",
            indice=1,
            categoria=categoria_equipo,
            tipo_objetivo=ReporteFalla.OBJETIVO_EQUIPO,
            activo=activo_uno,
            area_instalacion="",
            respuesta_activo=None,
        )
        repetido, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 26),
            observacion="No enciende",
            indice=2,
            categoria=categoria_equipo,
            tipo_objetivo=ReporteFalla.OBJETIVO_EQUIPO,
            activo=activo_dos,
            area_instalacion="",
            respuesta_activo=None,
        )

        propuestas = proponer_consolidacion_higiene()

        self.assertNotIn(
            (principal.id, repetido.id),
            {(row.principal_id, row.repetido_id) for row in propuestas.todas},
        )

    def test_reportes_de_distinta_sucursal_no_agrupan_aunque_compartan_sucursal_de_registro(self):
        otra_sucursal = Sucursal.objects.create(
            codigo="HIG-OTRA",
            nombre="Otra sucursal",
            activa=True,
        )
        principal, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion="No descarga agua",
            indice=1,
        )
        repetido, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 26),
            observacion="No descarga agua",
            indice=2,
            reporte_sucursal=otra_sucursal,
            registro_sucursal=self.sucursal,
        )

        propuestas = proponer_consolidacion_higiene()

        self.assertNotIn(
            (principal.id, repetido.id),
            {(row.principal_id, row.repetido_id) for row in propuestas.todas},
        )

    def test_discrepancia_entre_sucursal_de_reporte_y_registro_nunca_es_exacta_automatica(self):
        otra_sucursal = Sucursal.objects.create(
            codigo="HIG-MANUAL",
            nombre="Sucursal revisión manual",
            activa=True,
        )
        principal, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion="No descarga agua",
            indice=1,
            reporte_sucursal=otra_sucursal,
            registro_sucursal=self.sucursal,
        )
        repetido, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 26),
            observacion="No descarga agua",
            indice=2,
            reporte_sucursal=otra_sucursal,
            registro_sucursal=self.sucursal,
        )

        propuestas = proponer_consolidacion_higiene()

        self.assertNotIn(
            (principal.id, repetido.id),
            {(row.principal_id, row.repetido_id) for row in propuestas.exactas},
        )

    def test_reaparicion_despues_del_cierre_inicia_otro_ciclo(self):
        principal, posterior = self.crear_repeticiones_con_cierre_intermedio()

        propuestas = proponer_consolidacion_higiene()

        self.assertNotIn(
            (principal.id, posterior.id),
            {(row.principal_id, row.repetido_id) for row in propuestas.todas},
        )

    def test_resuelto_y_luego_cerrado_corta_el_ciclo_en_la_primera_transicion_terminal(self):
        principal, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion="No descarga agua",
            indice=1,
            fecha_reporte=timezone.make_aware(datetime(2026, 9, 25, 8, 0)),
        )
        resolucion = timezone.make_aware(datetime(2026, 9, 25, 12, 0))
        cierre = timezone.make_aware(datetime(2026, 9, 26, 18, 0))
        self._transicion(
            principal,
            anterior=ReporteFalla.ESTATUS_PROCESO,
            nuevo=ReporteFalla.ESTATUS_RESUELTO,
            cuando=resolucion,
        )
        self._transicion(
            principal,
            anterior=ReporteFalla.ESTATUS_RESUELTO,
            nuevo=ReporteFalla.ESTATUS_CERRADO,
            cuando=cierre,
        )
        principal.estatus = ReporteFalla.ESTATUS_CERRADO
        principal.fecha_resolucion = resolucion
        principal.fecha_cierre = cierre
        principal.save(update_fields=["estatus", "fecha_resolucion", "fecha_cierre"])
        posterior, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 26),
            observacion="No descarga agua",
            indice=2,
            fecha_reporte=timezone.make_aware(datetime(2026, 9, 26, 9, 0)),
        )

        propuestas = proponer_consolidacion_higiene()

        self.assertNotIn(
            (principal.id, posterior.id),
            {(row.principal_id, row.repetido_id) for row in propuestas.todas},
        )

    def test_reapertura_reactiva_el_ciclo_desde_su_transicion(self):
        principal, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion="No descarga agua",
            indice=1,
            fecha_reporte=timezone.make_aware(datetime(2026, 9, 25, 8, 0)),
        )
        cierre = timezone.make_aware(datetime(2026, 9, 25, 12, 0))
        reapertura = timezone.make_aware(datetime(2026, 9, 26, 8, 0))
        self._transicion(
            principal,
            anterior=ReporteFalla.ESTATUS_PROCESO,
            nuevo=ReporteFalla.ESTATUS_CERRADO,
            cuando=cierre,
        )
        self._transicion(
            principal,
            anterior=ReporteFalla.ESTATUS_CERRADO,
            nuevo=ReporteFalla.ESTATUS_ABIERTO,
            cuando=reapertura,
        )
        principal.estatus = ReporteFalla.ESTATUS_ABIERTO
        principal.fecha_cierre = cierre
        principal.save(update_fields=["estatus", "fecha_cierre"])
        repetido, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 26),
            observacion="No descarga agua",
            indice=2,
            fecha_reporte=timezone.make_aware(datetime(2026, 9, 26, 9, 0)),
        )

        propuestas = proponer_consolidacion_higiene()

        self.assertIn(
            (principal.id, repetido.id),
            {(row.principal_id, row.repetido_id) for row in propuestas.exactas},
        )

    def test_varios_ciclos_eligen_como_principal_el_inicio_del_ciclo_activo(self):
        primero, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 23),
            observacion="No descarga agua",
            indice=1,
        )
        primer_cierre = timezone.make_aware(datetime(2026, 9, 23, 18, 0))
        self._transicion(
            primero,
            anterior=ReporteFalla.ESTATUS_ABIERTO,
            nuevo=ReporteFalla.ESTATUS_CERRADO,
            cuando=primer_cierre,
        )
        primero.fecha_cierre = primer_cierre
        primero.estatus = ReporteFalla.ESTATUS_CERRADO
        primero.save(update_fields=["fecha_cierre", "estatus"])

        segundo, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 24),
            observacion="No descarga agua",
            indice=2,
        )
        segundo_cierre = timezone.make_aware(datetime(2026, 9, 24, 18, 0))
        self._transicion(
            segundo,
            anterior=ReporteFalla.ESTATUS_ABIERTO,
            nuevo=ReporteFalla.ESTATUS_RESUELTO,
            cuando=segundo_cierre,
        )
        segundo.fecha_resolucion = segundo_cierre
        segundo.estatus = ReporteFalla.ESTATUS_RESUELTO
        segundo.save(update_fields=["fecha_resolucion", "estatus"])

        tercero, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion="No descarga agua",
            indice=3,
        )
        repetido, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 26),
            observacion="No descarga agua",
            indice=4,
        )

        propuestas = proponer_consolidacion_higiene()
        pares = {(row.principal_id, row.repetido_id) for row in propuestas.exactas}

        self.assertEqual(pares, {(tercero.id, repetido.id)})

    def test_reapertura_recupera_un_ciclo_anterior_tras_otro_ciclo_ya_terminado(self):
        primero, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 22),
            observacion="No descarga agua",
            indice=1,
        )
        cierre_primero = timezone.make_aware(datetime(2026, 9, 22, 18, 0))
        self._transicion(
            primero,
            anterior=ReporteFalla.ESTATUS_ABIERTO,
            nuevo=ReporteFalla.ESTATUS_CERRADO,
            cuando=cierre_primero,
        )
        primero.fecha_cierre = cierre_primero
        primero.save(update_fields=["fecha_cierre"])

        intermedio, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 23),
            observacion="No descarga agua",
            indice=2,
        )
        cierre_intermedio = timezone.make_aware(datetime(2026, 9, 23, 18, 0))
        self._transicion(
            intermedio,
            anterior=ReporteFalla.ESTATUS_ABIERTO,
            nuevo=ReporteFalla.ESTATUS_RESUELTO,
            cuando=cierre_intermedio,
        )
        intermedio.fecha_resolucion = cierre_intermedio
        intermedio.save(update_fields=["fecha_resolucion"])

        reapertura = timezone.make_aware(datetime(2026, 9, 24, 8, 0))
        self._transicion(
            primero,
            anterior=ReporteFalla.ESTATUS_CERRADO,
            nuevo=ReporteFalla.ESTATUS_ABIERTO,
            cuando=reapertura,
        )
        repetido, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion="No descarga agua",
            indice=3,
        )

        propuestas = proponer_consolidacion_higiene()

        self.assertEqual(
            {(row.principal_id, row.repetido_id) for row in propuestas.exactas},
            {(primero.id, repetido.id)},
        )

    def test_estado_desconocido_no_reabre_un_ciclo_automaticamente(self):
        principal, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion="No descarga agua",
            indice=1,
            fecha_reporte=timezone.make_aware(datetime(2026, 9, 25, 8, 0)),
        )
        cierre = timezone.make_aware(datetime(2026, 9, 25, 12, 0))
        estado_legado = timezone.make_aware(datetime(2026, 9, 26, 8, 0))
        self._transicion(
            principal,
            anterior=ReporteFalla.ESTATUS_ABIERTO,
            nuevo=ReporteFalla.ESTATUS_CERRADO,
            cuando=cierre,
        )
        self._transicion(
            principal,
            anterior=ReporteFalla.ESTATUS_CERRADO,
            nuevo="estado_legado",
            cuando=estado_legado,
        )
        principal.fecha_cierre = cierre
        principal.estatus = "estado_legado"
        principal.save(update_fields=["fecha_cierre", "estatus"])
        posterior, _ = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 26),
            observacion="No descarga agua",
            indice=2,
            fecha_reporte=timezone.make_aware(datetime(2026, 9, 26, 9, 0)),
        )

        propuestas = proponer_consolidacion_higiene()

        self.assertNotIn(
            (principal.id, posterior.id),
            {(row.principal_id, row.repetido_id) for row in propuestas.todas},
        )

    def test_eventos_sin_transicion_no_alteran_ciclo_y_se_filtran_en_sql(self):
        principal, repetido, _, _ = self.crear_repeticiones(
            observaciones=("No descarga agua", "No descarga agua"),
        )
        BitacoraFalla.objects.create(
            reporte=principal,
            usuario=self.dg,
            estatus_anterior=ReporteFalla.ESTATUS_ABIERTO,
            estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
            comentario="Constatación sin cambio de estado.",
            timestamp=timezone.make_aware(datetime(2026, 9, 25, 12, 0)),
        )
        BitacoraFalla.objects.create(
            reporte=principal,
            usuario=self.dg,
            estatus_anterior="",
            estatus_nuevo="",
            comentario="Comentario operativo sin transición.",
            timestamp=timezone.make_aware(datetime(2026, 9, 25, 13, 0)),
        )

        with CaptureQueriesContext(connection) as consultas:
            propuestas = proponer_consolidacion_higiene()

        self.assertIn(
            (principal.id, repetido.id),
            {(row.principal_id, row.repetido_id) for row in propuestas.exactas},
        )
        self.assertEqual(len(consultas), 2)
        consulta_bitacora = consultas.captured_queries[1]["sql"].upper()
        self.assertIn("ESTATUS_NUEVO", consulta_bitacora)
        self.assertIn("ESTATUS_ANTERIOR", consulta_bitacora)
        self.assertIn("NOT", consulta_bitacora)

    @override_settings(TIME_ZONE="America/Mazatlan")
    def test_fechas_del_preview_se_muestran_en_fecha_local(self):
        principal, repetido, _, _ = self.crear_repeticiones(
            observaciones=("No descarga agua", "No descarga agua"),
        )
        principal.fecha_reporte = datetime(2026, 9, 26, 0, 30, tzinfo=ZoneInfo("UTC"))
        repetido.fecha_reporte = datetime(2026, 9, 27, 0, 30, tzinfo=ZoneInfo("UTC"))
        principal.save(update_fields=["fecha_reporte"])
        repetido.save(update_fields=["fecha_reporte"])

        propuesta = proponer_consolidacion_higiene().exactas[0]

        self.assertEqual(propuesta.fecha_principal, "2026-09-25")
        self.assertEqual(propuesta.fecha_repetida, "2026-09-26")

    def test_selecciona_una_sola_respuesta_deterministica_por_reporte_en_sql(self):
        principal, repetido, respuesta_principal, _ = self.crear_repeticiones(
            observaciones=("No descarga agua", "No descarga agua"),
        )
        RespuestaHigiene.objects.bulk_create(
            [
                RespuestaHigiene(
                    registro=respuesta_principal.registro,
                    punto_clave=f"otro-punto-{indice}",
                    seccion="Otra sección",
                    punto_revision=f"Otro punto {indice}",
                    respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
                    observacion="Otra observación",
                    reporte_falla=principal,
                )
                for indice in range(25)
            ]
        )

        with CaptureQueriesContext(connection) as consultas:
            propuestas = proponer_consolidacion_higiene()

        self.assertEqual(len(consultas), 2)
        self.assertEqual(propuestas.exactas[0].respuesta_principal_id, respuesta_principal.id)
        self.assertEqual(
            {(row.principal_id, row.repetido_id) for row in propuestas.todas},
            {(principal.id, repetido.id)},
        )
        consulta_respuestas = consultas.captured_queries[0]["sql"].upper()
        self.assertTrue(
            "DISTINCT ON" in consulta_respuestas or "SELECT U0." in consulta_respuestas,
            consulta_respuestas,
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

    def test_aplicar_par_conserva_campos_evidencia_estado_y_es_idempotente(self):
        principal, repetido, _, respuesta_repetida = self.crear_repeticiones(
            observaciones=("No descarga agua", "No descarga agua"),
        )
        repetido.refresh_from_db()
        evidencia = respuesta_repetida.evidencia.name
        snapshot = {
            field.attname: getattr(repetido, field.attname)
            for field in repetido._meta.concrete_fields
            if field.name != "duplicado_de"
        }

        primero = aplicar_consolidacion_higiene(
            [(principal.id, repetido.id)], actor=self.dg
        )
        segundo = aplicar_consolidacion_higiene(
            [(principal.id, repetido.id)], actor=self.dg
        )

        repetido.refresh_from_db()
        respuesta_repetida.refresh_from_db()
        self.assertEqual(repetido.duplicado_de_id, principal.id)
        self.assertEqual(primero.aplicados, 1)
        self.assertEqual(primero.omitidos, 0)
        self.assertEqual(segundo.aplicados, 0)
        self.assertEqual(segundo.omitidos, 1)
        self.assertEqual(respuesta_repetida.evidencia.name, evidencia)
        self.assertEqual(
            {
                field.attname: getattr(repetido, field.attname)
                for field in repetido._meta.concrete_fields
                if field.name != "duplicado_de"
            },
            snapshot,
        )
        self.assertEqual(
            BitacoraFalla.objects.filter(
                reporte_id__in=[principal.id, repetido.id]
            ).count(),
            2,
        )
        self.assertEqual(
            AuditLog.objects.filter(
                action="CONSOLIDATE",
                model="fallas.ReporteFalla",
                object_id=str(repetido.id),
            ).count(),
            1,
        )

    def test_aplicar_revalida_preview_y_omite_par_que_dejo_de_ser_exacto(self):
        principal, repetido, _, respuesta_repetida = self.crear_repeticiones(
            observaciones=("No descarga agua", "No descarga agua"),
        )
        self.assertEqual(len(proponer_consolidacion_higiene().exactas), 1)
        respuesta_repetida.observacion = "Ahora también pierde agua"
        respuesta_repetida.save(update_fields=["observacion"])

        resultado = aplicar_consolidacion_higiene(
            [(principal.id, repetido.id)], actor=self.dg
        )

        repetido.refresh_from_db()
        self.assertEqual(resultado.aplicados, 0)
        self.assertEqual(resultado.omitidos, 1)
        self.assertIsNone(repetido.duplicado_de_id)
        self.assertFalse(AuditLog.objects.filter(action="CONSOLIDATE").exists())

    def test_aplicar_deduplica_la_misma_seleccion_en_una_solicitud(self):
        principal, repetido, _, _ = self.crear_repeticiones(
            observaciones=("No descarga agua", "No descarga agua"),
        )
        pair = (principal.id, repetido.id)

        resultado = aplicar_consolidacion_higiene([pair, pair], actor=self.dg)

        self.assertEqual(resultado.aplicados, 1)
        self.assertEqual(resultado.omitidos, 0)
        self.assertEqual(AuditLog.objects.filter(action="CONSOLIDATE").count(), 1)

    def test_aplicar_revierte_enlace_y_bitacoras_si_falla_la_auditoria(self):
        principal, repetido, _, _ = self.crear_repeticiones(
            observaciones=("No descarga agua", "No descarga agua"),
        )

        with patch(
            "operacion.services_higiene_consolidacion.AuditLog.objects.create",
            side_effect=RuntimeError("auditoría no disponible"),
        ):
            with self.assertRaisesRegex(RuntimeError, "auditoría no disponible"):
                aplicar_consolidacion_higiene(
                    [(principal.id, repetido.id)], actor=self.dg
                )

        repetido.refresh_from_db()
        self.assertIsNone(repetido.duplicado_de_id)
        self.assertFalse(
            BitacoraFalla.objects.filter(
                reporte_id__in=[principal.id, repetido.id]
            ).exists()
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


class ConsolidacionHigieneConcurrencyTests(TransactionTestCase):
    def setUp(self):
        users = get_user_model()
        self.dg = users.objects.create_superuser(
            username="dg.concurrencia", password="test"
        )
        operadora = users.objects.create_user(username="higiene.concurrencia")
        sucursal = Sucursal.objects.create(
            codigo="HIG-CONCUR",
            nombre="Sucursal concurrencia",
            activa=True,
        )
        categoria = CategoriaFalla.objects.create(
            nombre="Plomería concurrencia",
            tipo=CategoriaFalla.TIPO_INSTALACION,
        )
        reportes = []
        for indice, fecha in enumerate((date(2026, 9, 25), date(2026, 9, 26)), 1):
            reporte = ReporteFalla.objects.create(
                sucursal=sucursal,
                categoria=categoria,
                tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
                area_instalacion="Baños",
                titulo="Limpieza de baños · Sanitario limpio y funcional",
                descripcion="Hallazgo de higiene: No descarga agua",
                justificacion_sin_foto="Prueba automatizada.",
                reportado_por=operadora,
                fecha_reporte=timezone.make_aware(
                    datetime.combine(fecha, datetime.min.time())
                ),
            )
            registro = RegistroHigiene.objects.create(
                tipo=RegistroHigiene.TIPO_BANOS,
                sucursal=sucursal,
                fecha=fecha,
                clave_instancia=f"clientes-ronda-{indice}",
                plantilla_version="2026.1",
                creado_por=operadora,
            )
            RespuestaHigiene.objects.create(
                registro=registro,
                punto_clave="banos_sanitario",
                seccion="Limpieza de baños",
                punto_revision="Sanitario limpio y funcional",
                respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
                observacion="No descarga agua",
                requiere_seguimiento=True,
                tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
                area_instalacion="Baños",
                reporte_falla=reporte,
                continuidad_falla=RespuestaHigiene.CONTINUIDAD_INICIAL,
            )
            reportes.append(reporte)
        self.pair = (reportes[0].id, reportes[1].id)

    def test_dos_aplicaciones_concurrentes_generan_un_solo_enlace_y_una_auditoria(self):
        barrier = Barrier(2)
        resultados = []
        errores = []

        def aplicar():
            close_old_connections()
            try:
                actor = get_user_model().objects.get(pk=self.dg.pk)
                barrier.wait(timeout=5)
                resultados.append(
                    aplicar_consolidacion_higiene([self.pair], actor=actor)
                )
            except Exception as exc:  # pragma: no cover - se afirma fuera del hilo
                errores.append(exc)
            finally:
                close_old_connections()

        threads = [Thread(target=aplicar), Thread(target=aplicar)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertFalse(errores)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(sum(row.aplicados for row in resultados), 1)
        self.assertEqual(sum(row.omitidos for row in resultados), 1)
        self.assertEqual(AuditLog.objects.filter(action="CONSOLIDATE").count(), 1)
        self.assertEqual(BitacoraFalla.objects.count(), 2)
