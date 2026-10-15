from datetime import date
from unittest.mock import patch

from django.test import TestCase, override_settings

from sat_client.models import SolicitudDescarga
from sat_client.services.solicitud import buscar_solicitud_vigente
from sat_client.services.verificacion import verificar_hasta_terminar
from sat_client.tasks import _solicitud_periodo_registrada


@override_settings(SAT_RFC="AAA010101AAA")
class SatCoberturaTests(TestCase):
    def request(self, **kwargs):
        return SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 9, 17),
            fecha_final=date(2026, 9, 17),
            rfc_solicitante="AAA010101AAA",
            direccion="emitidos",
            **kwargs,
        )

    def test_timeout_keeps_request_resumable(self):
        s = self.request(id_solicitud="pending", estado=2)
        with patch(
            "sat_client.services.verificacion.verificar_solicitud", return_value=s
        ):
            verificar_hasta_terminar(
                s, obtener_token_func=lambda: "token", max_intentos=1
            )
        s.refresh_from_db()
        self.assertEqual(s.estado, 2)

    def test_missing_request_with_zero_state_is_expired(self):
        from sat_client.services.verificacion import verificar_solicitud

        s = self.request(id_solicitud="expired", estado=1)
        with patch("sat_client.services.verificacion._build_verificacion_envelope"), patch(
            "sat_client.services.verificacion.post_soap", return_value=b"response"
        ), patch("sat_client.services.verificacion.find_result_attributes", return_value={
            "CodEstatus": "5004", "EstadoSolicitud": "0", "NumeroCFDIs": "0",
            "Mensaje": "No se encontro la informacion",
        }):
            verificar_solicitud(s, token="token")
        s.refresh_from_db()
        self.assertEqual(s.estado, SolicitudDescarga.ESTADO_VENCIDA)

    def test_legacy_timeout_is_reused(self):
        s = self.request(
            id_solicitud="old",
            estado=4,
            error_detalle="Timeout esperando terminacion de solicitud SAT",
        )
        found = buscar_solicitud_vigente(
            fecha_inicial=s.fecha_inicial,
            fecha_final=s.fecha_final,
            rfc_solicitante=s.rfc_solicitante,
            tipo_solicitud="CFDI",
            direccion="emitidos",
        )
        self.assertEqual(found, s)

    def test_quota_failure_is_not_coverage(self):
        self.request(estado=5, codigo_estado="5002")
        self.assertFalse(
            _solicitud_periodo_registrada(
                date(2026, 9, 17), date(2026, 9, 17), "emitidos"
            )
        )

    def test_active_request_needs_processing(self):
        self.request(estado=2, id_solicitud="pending")
        self.assertFalse(
            _solicitud_periodo_registrada(
                date(2026, 9, 17), date(2026, 9, 17), "emitidos"
            )
        )


class SatOriginalPreservationTests(TestCase):
    def test_reimport_preserves_payment_identity(self):
        from sat_client.services.descarga import guardar_cfdis_xml
        from sat_client.models import CfdiPagoRelacionado
        from sat_client.tests.test_descarga import CFDI_PAGO_XML

        guardar_cfdis_xml([CFDI_PAGO_XML], solicitud=None, tipo_cfdi="emitido")
        pk = CfdiPagoRelacionado.objects.get().pk
        guardar_cfdis_xml([CFDI_PAGO_XML], solicitud=None, tipo_cfdi="emitido")
        self.assertEqual(CfdiPagoRelacionado.objects.get().pk, pk)

    def test_failed_package_rolls_back(self):
        from sat_client.services.descarga import guardar_cfdis_xml
        from sat_client.models import CfdiDescargado
        from sat_client.tests.test_descarga import CFDI_XML

        with self.assertRaises(Exception):
            guardar_cfdis_xml(
                [CFDI_XML, b"<broken/>"], solicitud=None, tipo_cfdi="emitido"
            )
        self.assertFalse(CfdiDescargado.objects.exists())


class SatMetadataTests(TestCase):
    def test_metadata_detects_late_document_and_validates_rfc(self):
        import base64, io, zipfile
        from sat_client.services import descarga

        self.assertTrue(
            hasattr(descarga, "extraer_metadata"), "Falta inventario metadata SAT"
        )
        data = (
            "Uuid~RfcEmisor~RfcReceptor~FechaEmision~EfectoComprobante~Estatus\n"
            "550E8400-E29B-41D4-A716-446655440000~AAA010101AAA~BBB010101BBB~2026-09-17 12:00:00~I~1\n"
        )
        b = io.BytesIO()
        with zipfile.ZipFile(b, "w") as z:
            z.writestr("metadata.txt", data)
            z.writestr("metadata_tercero.txt", "Uuid~RfcACuentaTerceros~NombreACuentaTerceros\n")
        rows = descarga.extraer_metadata(
            base64.b64encode(b.getvalue()).decode(),
            rfc="AAA010101AAA",
            direccion="emitidos",
        )
        self.assertEqual(rows[0]["uuid"], "550E8400-E29B-41D4-A716-446655440000")
        self.assertEqual(rows[0]["estatus"], "vigente")
        self.assertEqual(len(rows), 1)
        with self.assertRaises(Exception):
            descarga.extraer_metadata(
                base64.b64encode(b.getvalue()).decode(),
                rfc="OTHER",
                direccion="emitidos",
            )

    def test_auxiliary_metadata_without_main_inventory_is_rejected(self):
        import base64, io, zipfile
        from sat_client.services.descarga import extraer_metadata
        from sat_client.services.base import SatServiceError

        b = io.BytesIO()
        with zipfile.ZipFile(b, "w") as z:
            z.writestr("metadata_tercero.txt", "Uuid~RfcACuentaTerceros~NombreACuentaTerceros\n")
        with self.assertRaises(SatServiceError):
            extraer_metadata(base64.b64encode(b.getvalue()).decode(),
                             rfc="AAA010101AAA", direccion="emitidos")

    def test_metadata_request_does_not_filter_cancelled(self):
        from lxml import etree
        from sat_client.services.solicitud import _build_solicitud_envelope
        from sat_client.services.base import SatCredentials

        captured = {}

        def signed(tag, attrs, creds):
            captured.update(attrs)
            return etree.Element(tag)

        with patch(
            "sat_client.services.solicitud.get_sat_credentials",
            return_value=SatCredentials("cer", "key", "pw", "AAA010101AAA"),
        ), patch(
            "sat_client.services.solicitud.build_signed_sat_request", side_effect=signed
        ):
            _build_solicitud_envelope(
                fecha_inicial=date(2026, 9, 1),
                fecha_final=date(2026, 9, 30),
                direccion="emitidos",
                tipo_solicitud="Metadata",
            )
        self.assertNotIn("EstadoComprobante", captured)
        self.assertNotIn("TipoComprobante", captured)


class SatPackageProgressTests(TestCase):
    def test_failed_parse_reuses_downloaded_payload(self):
        from sat_client import tasks
        from sat_client.services.base import SatServiceError

        s = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 9, 1), fecha_final=date(2026, 9, 30),
            rfc_solicitante="AAA010101AAA", direccion="recibidos",
            tipo_solicitud="Metadata", estado=3, id_solicitud="cached",
            numero_cfdis=1, ids_paquetes=["p"],
        )
        rows = [{"uuid": "550E8400-E29B-41D4-A716-446655440000"}]
        with patch("sat_client.tasks.obtener_token", return_value="token"), patch(
            "sat_client.tasks.descargar_paquete", return_value="payload"
        ) as download, patch("sat_client.tasks.extraer_metadata", side_effect=[
            SatServiceError("parser failure"), rows
        ]):
            with self.assertRaises(SatServiceError):
                tasks.procesar_solicitud(s)
            s.refresh_from_db()
            self.assertEqual(s.paquetes_pendientes, {"p": "payload"})
            self.assertEqual(s.paquetes_procesados, {})
            result = tasks.procesar_solicitud(s)
        self.assertEqual(download.call_count, 1)
        s.refresh_from_db()
        self.assertEqual(result["estado"], "completa")
        self.assertEqual(s.metadata, rows)
        self.assertEqual(s.paquetes_pendientes, {})

    def test_exhausted_package_becomes_unavailable(self):
        from sat_client import tasks
        from sat_client.services.base import SatServiceError

        s = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 9, 1), fecha_final=date(2026, 9, 30),
            rfc_solicitante="AAA010101AAA", direccion="recibidos",
            tipo_solicitud="Metadata", estado=3, id_solicitud="exhausted",
            numero_cfdis=1, ids_paquetes=["p"],
        )
        with patch("sat_client.tasks.obtener_token", return_value="token"), patch(
            "sat_client.tasks.descargar_paquete",
            side_effect=SatServiceError("Maximo de descargas permitidas", code="5008"),
        ):
            result = tasks.procesar_solicitud(s)
        s.refresh_from_db()
        self.assertEqual(s.estado, SolicitudDescarga.ESTADO_VENCIDA)
        self.assertEqual(s.codigo_estado, "5008")
        self.assertEqual(result["estado"], "bloqueada")

    def test_expired_metadata_is_renewed_even_if_recent(self):
        from sat_client import tasks

        old = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 9, 1), fecha_final=date(2026, 9, 1),
            rfc_solicitante="AAA010101AAA", direccion="emitidos",
            tipo_solicitud="Metadata", estado=6, id_solicitud="expired",
        )
        SolicitudDescarga.objects.create(
            fecha_inicial=old.fecha_inicial, fecha_final=old.fecha_final,
            rfc_solicitante=old.rfc_solicitante, direccion="recibidos",
            tipo_solicitud="Metadata", estado=3, id_solicitud="complete",
            numero_cfdis=0, metadata=[],
        )
        with override_settings(SAT_RFC=old.rfc_solicitante), patch(
            "sat_client.tasks.timezone.localdate", return_value=date(2026, 9, 2)
        ), patch("sat_client.tasks.obtener_token", return_value="token"), patch(
            "sat_client.tasks.solicitar_descarga_periodo", return_value=old
        ) as request:
            tasks.actualizar_cobertura_sat(desde=old.fecha_inicial)
        request.assert_called_once()
        self.assertEqual(request.call_args.kwargs["direccion"], "emitidos")

    def test_resume_does_not_download_imported_package(self):
        from sat_client import tasks

        self.assertTrue(
            hasattr(tasks, "procesar_solicitud"), "Falta reanudacion por ID y paquete"
        )
        s = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 5, 31),
            fecha_final=date(2026, 5, 31),
            rfc_solicitante="AAA010101AAA",
            direccion="emitidos",
            estado=3,
            id_solicitud="s",
            numero_cfdis=1,
            ids_paquetes=["p"],
            paquetes_procesados={"p": 1},
        )
        with patch("sat_client.tasks.descargar_paquete") as download:
            result = tasks.procesar_solicitud(s)
        download.assert_not_called()
        self.assertEqual(result["estado"], "completa")

    def test_folio_request_is_never_range_coverage(self):
        self.assertIn("folio", {f.name for f in SolicitudDescarga._meta.fields})
        from sat_client.models import LogDescargaSat

        s = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 9, 17),
            fecha_final=date(2026, 9, 17),
            rfc_solicitante="AAA010101AAA",
            direccion="emitidos",
            estado=3,
            id_solicitud="one",
            numero_cfdis=1,
            folio="550E8400-E29B-41D4-A716-446655440000",
        )
        LogDescargaSat.objects.create(
            solicitud=s, nivel="INFO", mensaje="ok", cfdis_descargados=1
        )
        with override_settings(SAT_RFC="AAA010101AAA"):
            self.assertFalse(
                _solicitud_periodo_registrada(
                    s.fecha_inicial, s.fecha_final, "emitidos"
                )
            )


class SatReconciliationTests(TestCase):
    def test_late_uuid_is_selected_after_old_download(self):
        from sat_client import tasks

        self.assertTrue(
            hasattr(tasks, "conciliar_metadata"), "Falta conciliacion por UUID"
        )
        s = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 9, 1),
            fecha_final=date(2026, 9, 30),
            rfc_solicitante="AAA010101AAA",
            direccion="emitidos",
            tipo_solicitud="Metadata",
            estado=3,
            id_solicitud="meta",
            numero_cfdis=1,
            paquetes_procesados={"p": 1},
            ids_paquetes=["p"],
            metadata=[
                {
                    "uuid": "550E8400-E29B-41D4-A716-446655440000",
                    "estatus": "vigente",
                    "fecha_emision": "2026-09-17T12:00:00-07:00",
                    "datos": {},
                }
            ],
        )
        with patch("sat_client.tasks.solicitar_descarga_periodo") as request, patch(
            "sat_client.tasks.procesar_solicitud",
            return_value={"estado": "pendiente", "nuevos": 0, "descargados": 0},
        ), patch("sat_client.tasks.obtener_token", return_value="token"):
            result = tasks.conciliar_metadata([s])
        self.assertEqual(
            request.call_args.kwargs["folio"], "550E8400-E29B-41D4-A716-446655440000"
        )
        self.assertEqual(result["faltantes"], 1)

    def test_cancelled_metadata_preserves_original_and_does_not_request(self):
        from sat_client import tasks

        self.assertTrue(
            hasattr(tasks, "conciliar_metadata"), "Falta conciliacion por UUID"
        )
        from sat_client.models import CfdiDescargado
        from sat_client.services.descarga import guardar_cfdis_xml
        from sat_client.tests.test_descarga import CFDI_XML

        guardar_cfdis_xml([CFDI_XML], solicitud=None, tipo_cfdi="emitido")
        cf = CfdiDescargado.objects.get()
        cf.conciliado = True
        cf.save()
        s = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 5, 1),
            fecha_final=date(2026, 5, 31),
            rfc_solicitante="AAA010101AAA",
            direccion="emitidos",
            tipo_solicitud="Metadata",
            estado=3,
            id_solicitud="meta",
            numero_cfdis=1,
            paquetes_procesados={"p": 1},
            ids_paquetes=["p"],
            metadata=[
                {
                    "uuid": cf.uuid,
                    "estatus": "cancelado",
                    "fecha_emision": "2026-05-31T12:30:00-07:00",
                    "datos": {},
                }
            ],
        )
        with patch("sat_client.tasks.solicitar_descarga_periodo") as request:
            tasks.conciliar_metadata([s])
        request.assert_not_called()
        cf.refresh_from_db()
        self.assertEqual(cf.estatus, "cancelado")
        self.assertTrue(cf.conciliado)
        self.assertTrue(cf.xml_raw)


class SatIntegrationCoverageTests(TestCase):
    def test_all_invoice_types_are_preserved(self):
        from sat_client.services.descarga import guardar_cfdis_xml
        from sat_client.tests.test_descarga import CFDI_XML
        from sat_client.models import CfdiDescargado

        for i, tipo in enumerate("IENPT"):
            xml = CFDI_XML.replace(
                b'TipoDeComprobante="I"', f'TipoDeComprobante="{tipo}"'.encode()
            ).replace(b"446655440000", f"44665544000{i}".encode())
            guardar_cfdis_xml([xml], solicitud=None, tipo_cfdi="emitido")
        self.assertEqual(
            set(CfdiDescargado.objects.values_list("tipo_comprobante", flat=True)),
            set("IENPT"),
        )

    def test_partial_metadata_is_not_used(self):
        from sat_client.tasks import conciliar_metadata

        s = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 9, 1),
            fecha_final=date(2026, 9, 30),
            rfc_solicitante="AAA010101AAA",
            direccion="emitidos",
            tipo_solicitud="Metadata",
            estado=3,
            id_solicitud="partial",
            numero_cfdis=2,
            metadata=[{"uuid": "550E8400-E29B-41D4-A716-446655440000"}],
            paquetes_procesados={"p": 1},
        )
        with patch("sat_client.tasks.solicitar_descarga_periodo") as request:
            result = conciliar_metadata([s])
        self.assertEqual(result["inventario"], 0)
        request.assert_not_called()

    def test_mismatched_package_does_not_create_original(self):
        from sat_client.tasks import procesar_solicitud
        from sat_client.tests.test_descarga import CFDI_XML
        from sat_client.models import CfdiDescargado

        s = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 5, 31),
            fecha_final=date(2026, 5, 31),
            rfc_solicitante="OTHER",
            direccion="emitidos",
            estado=3,
            id_solicitud="wrong-rfc",
            numero_cfdis=1,
            ids_paquetes=["p"],
        )
        with patch("sat_client.tasks.obtener_token", return_value="token"), patch(
            "sat_client.tasks.descargar_paquete", return_value="zip"
        ), patch(
            "sat_client.tasks.extraer_xmls_de_zip_base64", return_value=[CFDI_XML]
        ):
            with self.assertRaises(Exception):
                procesar_solicitud(s)
        self.assertFalse(CfdiDescargado.objects.exists())
        s.refresh_from_db()
        self.assertEqual(s.paquetes_procesados, {})

    def test_monthly_inventory_reuses_active_request(self):
        from sat_client.tasks import actualizar_cobertura_sat
        from django.utils import timezone

        s = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 9, 1),
            fecha_final=date(2026, 9, 29),
            rfc_solicitante="AAA010101AAA",
            direccion="emitidos",
            tipo_solicitud="Metadata",
            estado=2,
            id_solicitud="active",
        )
        with override_settings(SAT_RFC="AAA010101AAA"), patch(
            "sat_client.tasks.timezone.localdate", return_value=date(2026, 10, 1)
        ), patch("sat_client.tasks.obtener_token", return_value="token"), patch(
            "sat_client.tasks.procesar_solicitud",
            return_value={"estado": "pendiente", "nuevos": 0},
        ) as process, patch(
            "sat_client.tasks.solicitar_descarga_periodo", return_value=s
        ) as request:
            result = actualizar_cobertura_sat(desde=date(2026, 9, 1))
        self.assertEqual(request.call_count, 1)
        self.assertEqual(request.call_args.kwargs["direccion"], "recibidos")
        self.assertIn(s.pk, result["pendientes"])


class SatHistoricalResumeTests(TestCase):
    @override_settings(SAT_RFC="AAA010101AAA", SAT_DESCARGA_ENABLED=True)
    def test_monthly_pending_request_is_resumed_even_when_days_are_imported(self):
        s = SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 9, 1),
            fecha_final=date(2026, 9, 30),
            rfc_solicitante="AAA010101AAA",
            direccion="recibidos",
            estado=2,
            id_solicitud="monthly",
        )
        with patch(
            "sat_client.tasks.periodos_diarios_a_descargar", return_value=[]
        ), patch(
            "sat_client.tasks.procesar_solicitud",
            return_value={"estado": "pendiente", "nuevos": 0, "descargados": 0},
        ) as process, patch(
            "sat_client.tasks.actualizar_cobertura_sat",
            return_value={"faltantes": 0, "pendientes": [], "errores": []},
        ):
            from sat_client.tasks import ejecutar_descarga_sat_nocturna

            result = ejecutar_descarga_sat_nocturna.run()
        process.assert_called_once_with(s)
        self.assertEqual(result["status"], "parcial")

class SatFolioContractTests(TestCase):
    def test_folio_uses_official_operation_without_date_filters(self):
        from lxml import etree
        from sat_client.services.solicitud import _build_solicitud_envelope
        from sat_client.services.base import SatCredentials,find_first_element
        captured={}
        def signed(tag,attrs,creds):
            captured.update(attrs)
            return etree.Element(tag)
        with patch('sat_client.services.solicitud.get_sat_credentials',return_value=SatCredentials('cer','key','pw','AAA010101AAA')),patch('sat_client.services.solicitud.build_signed_sat_request',side_effect=signed):
            xml=_build_solicitud_envelope(fecha_inicial=date(2026,9,1),fecha_final=date(2026,9,1),direccion='recibidos',tipo_solicitud='CFDI',folio='550E8400-E29B-41D4-A716-446655440000')
        self.assertIsNotNone(find_first_element(xml,'SolicitaDescargaFolio'))
        self.assertEqual(set(captured),{'RfcSolicitante','Folio'})

    def test_quota_rejection_never_creates_another_request(self):
        from sat_client.services.solicitud import solicitar_descarga_periodo
        from sat_client.services.base import SatCredentials
        s=SolicitudDescarga.objects.create(fecha_inicial=date(2026,9,17),fecha_final=date(2026,9,17),rfc_solicitante='AAA010101AAA',direccion='emitidos',estado=5,codigo_estado='5002')
        with patch('sat_client.services.solicitud.get_sat_credentials',return_value=SatCredentials('cer','key','pw','AAA010101AAA')),patch('sat_client.services.solicitud.post_soap') as post:
            found=solicitar_descarga_periodo(fecha_inicial=s.fecha_inicial,fecha_final=s.fecha_final,direccion=s.direccion,token='token')
        self.assertEqual(found.pk,s.pk);post.assert_not_called()
