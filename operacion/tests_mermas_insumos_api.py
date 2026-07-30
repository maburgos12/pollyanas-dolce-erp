import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.core import mail
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal, UserModuleAccess, UserProfile
from mermas.models import MermaInsumo, OrdenAjustePoint
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob, PointTransferLine
from pos_bridge.services.live_inventory_lookup_service import PointLiveInventoryLookupError
from rrhh.models import Empleado


User = get_user_model()


class OperacionMermasInsumosApiTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.user = User.objects.create_user(username="encargada.payan", email="encargada@example.com")
        self.jefe_user = User.objects.create_user(username="jefe.payan", email="jefe@example.com")
        UserProfile.objects.create(user=self.user, sucursal=self.sucursal)
        UserProfile.objects.create(user=self.jefe_user, sucursal=self.sucursal)
        self.jefe = Empleado.objects.create(
            nombre="Jefa Payán", usuario_erp=self.jefe_user, sucursal_ref=self.sucursal
        )
        Empleado.objects.create(
            nombre="Encargada Payán",
            usuario_erp=self.user,
            sucursal_ref=self.sucursal,
            jefe_directo=self.jefe,
        )
        self.cedis = PointBranch.objects.create(external_id="1", name="CEDIS")
        self.branch = PointBranch.objects.create(external_id="2", name="Payán", erp_branch=self.sucursal)
        self.job = PointSyncJob.objects.create(status=PointSyncJob.STATUS_SUCCESS)
        self.product = PointProduct.objects.create(external_id="p-1", sku="INS-001", name="Fresa fresca")
        PointTransferLine.objects.create(
            origin_branch=self.cedis,
            destination_branch=self.branch,
            erp_destination_branch=self.sucursal,
            sync_job=self.job,
            transfer_external_id="T-1",
            detail_external_id="D-1",
            source_hash="hash-1",
            registered_at=timezone.now(),
            received_at=timezone.now(),
            item_name="Fresa fresca",
            item_code="INS-001",
            unit="KG",
            received_quantity=Decimal("8.250"),
            is_insumo=True,
            is_received=True,
            is_current_snapshot=True,
        )
        PointInventorySnapshot.objects.create(
            branch=self.branch,
            product=self.product,
            stock=Decimal("8.250"),
            sync_job=self.job,
        )
        self.live_stock = SimpleNamespace(
            codigo_point="INS-001",
            nombre_point="Fresa fresca",
            unidad_point="KG",
            existencia=Decimal("8.250"),
            snapshot_capturado_en=timezone.now(),
        )
        self.view_live_lookup = patch(
            "operacion.views.consultar_existencia_insumo_point",
            return_value=self.live_stock,
        )
        self.service_live_lookup = patch(
            "mermas.services_insumos.consultar_existencia_insumo_point",
            return_value=self.live_stock,
        )
        self.view_live_lookup.start()
        self.service_live_lookup.start()
        self.addCleanup(self.view_live_lookup.stop)
        self.addCleanup(self.service_live_lookup.stop)
        self.client.force_login(self.user)

    def payload(self, **overrides):
        data = {
            "codigo_point": "INS-001",
            "cantidad": "3.000",
            "motivo": "DESCOMPOSICION",
            "comentario": "Fresa demasiado madura",
            "justificacion_sin_foto": "Se desechó durante la apertura",
        }
        data.update(overrides)
        return data

    def habilitar_usuario_solo_mermas(self):
        UserModuleAccess.objects.create(
            user=self.user,
            module="mermas.captura",
            access=UserModuleAccess.ACCESS_MANAGE,
        )

    def test_usuario_solo_mermas_puede_abrir_captura_de_insumos(self):
        self.habilitar_usuario_solo_mermas()

        response = self.client.get(reverse("operacion:sucursal_tools"), {"tab": "mermas"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Registrar merma de insumo")

    def test_usuario_solo_mermas_puede_enviar_merma_de_insumo(self):
        self.habilitar_usuario_solo_mermas()

        response = self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(MermaInsumo.objects.count(), 1)

    def test_catalogo_y_creacion_derivan_sucursal_point_y_jefe_rrhh(self):
        catalogo = self.client.get(reverse("operacion:mermas_insumos_catalogo_api"))
        self.assertEqual(catalogo.status_code, 200)
        self.assertEqual(catalogo.json()["insumos"][0]["codigo_point"], "INS-001")

        response = self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        merma = MermaInsumo.objects.get()
        self.assertEqual(merma.sucursal, self.sucursal)
        self.assertEqual(merma.estatus, MermaInsumo.ESTATUS_ENVIADA)
        self.assertEqual(merma.jefe_inmediato, self.jefe_user)
        self.assertEqual(merma.unidad_point, "KG")

    def test_catalogo_muestra_insumo_recibido_sin_snapshot_local(self):
        PointInventorySnapshot.objects.all().delete()

        catalogo = self.client.get(reverse("operacion:mermas_insumos_catalogo_api"))

        self.assertEqual(catalogo.status_code, 200)
        self.assertEqual(
            catalogo.json()["insumos"],
            [{
                "codigo_point": "INS-001",
                "nombre": "Fresa fresca",
                "unidad": "KG",
                "existencia": None,
                "snapshot_en": None,
            }],
        )

    def test_formulario_configura_consulta_point_al_seleccionar_insumo(self):
        pagina = self.client.get(
            reverse("operacion:sucursal_tools"),
            {"tab": "mermas"},
        )

        self.assertEqual(pagina.status_code, 200)
        self.assertContains(
            pagina,
            f'data-stock-url="{reverse("operacion:mermas_insumos_catalogo_api")}"',
        )
        self.assertContains(pagina, "data-catalog-status")
        self.assertContains(pagina, "20260730-sales-catalog-live-v3")
        self.assertContains(
            pagina,
            'navigator.serviceWorker.register("/app/sw.js?v=20260730-sales-catalog-live-v5"',
        )
        self.assertContains(pagina, 'updateViaCache: "none"')
        self.assertContains(pagina, "La existencia se consulta directamente en Point")

    def test_formulario_reemplaza_catalogo_antiguo_aunque_no_este_vacio(self):
        root = Path(__file__).resolve().parents[1]
        javascript = (root / "static/operacion/sucursal_tools.js").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("supply.options.length > 1", javascript)
        self.assertIn("supply.replaceChildren(placeholder, fragment)", javascript)

    @patch("operacion.views.consultar_existencia_insumo_point")
    def test_catalogo_consulta_existencia_seleccionada_directamente_en_point(self, mock_consultar):
        captured_at = timezone.now()
        mock_consultar.return_value = SimpleNamespace(
            codigo_point="INS-001",
            nombre_point="Fresa fresca",
            unidad_point="KG",
            existencia=Decimal("4.500"),
            snapshot_capturado_en=captured_at,
        )

        response = self.client.get(
            reverse("operacion:mermas_insumos_catalogo_api"),
            {"codigo_point": "INS-001"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["insumo"]["existencia"], "4.500")
        self.assertEqual(response.json()["insumo"]["unidad"], "KG")
        mock_consultar.assert_called_once_with(self.sucursal, "INS-001")

    @patch("operacion.views.consultar_existencia_insumo_point")
    def test_creacion_revalida_existencia_directamente_en_point(self, mock_consultar):
        PointInventorySnapshot.objects.all().delete()
        mock_consultar.return_value = SimpleNamespace(
            codigo_point="INS-001",
            nombre_point="Fresa fresca",
            unidad_point="KG",
            existencia=Decimal("4.500"),
            snapshot_capturado_en=timezone.now(),
        )

        response = self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload(cantidad="3.000")),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(MermaInsumo.objects.get().cantidad_reportada, Decimal("3.000"))
        mock_consultar.assert_called_once()
        args, kwargs = mock_consultar.call_args
        self.assertEqual(args, (self.sucursal, "INS-001"))
        self.assertEqual(kwargs["insumo_recibido"].codigo_point, "INS-001")

    @patch(
        "operacion.views.consultar_existencia_insumo_point",
        side_effect=PointLiveInventoryLookupError("Point no respondió"),
    )
    def test_point_en_vivo_no_disponible_no_registra_merma(self, _mock_consultar):
        response = self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 503)
        self.assertFalse(MermaInsumo.objects.exists())

    @patch("operacion.views.consultar_existencia_insumo_point")
    def test_cantidad_superior_a_point_no_registra_merma(self, mock_consultar):
        mock_consultar.return_value = SimpleNamespace(
            codigo_point="INS-001",
            nombre_point="Fresa fresca",
            unidad_point="KG",
            existencia=Decimal("2.000"),
            snapshot_capturado_en=timezone.now(),
        )

        response = self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload(cantidad="3.000")),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(MermaInsumo.objects.exists())

    @patch("operacion.views._cargar_insumos_sucursal", return_value=([], True))
    def test_timeout_catalogo_muestra_fallback_y_no_escribe(self, _mock_cargar):
        pagina = self.client.get(reverse("operacion:sucursal_tools"), {"tab": "mermas"})
        self.assertEqual(pagina.status_code, 200)
        self.assertContains(pagina, "No pudimos cargar las existencias de Point")
        self.assertNotContains(pagina, 'id="merma-form"')

        catalogo = self.client.get(reverse("operacion:mermas_insumos_catalogo_api"))
        self.assertEqual(catalogo.status_code, 503)

        crear = self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()),
            content_type="application/json",
        )
        self.assertEqual(crear.status_code, 503)
        self.assertEqual(MermaInsumo.objects.count(), 0)

    def test_envio_notifica_por_correo_al_jefe_sin_afectar_transaccion(self):
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(
                reverse("operacion:mermas_insumos_crear_api"),
                data=json.dumps(self.payload()), content_type="application/json",
            )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].to, ["jefe@example.com"])
        self.assertIn("Merma por aprobar", mail.outbox[0].subject)

    def test_sin_responsable_avisa_a_direccion_y_no_afirma_envio_a_jefe(self):
        sin_rrhh = User.objects.create_user(username="sin.rrhh", email="sinrrhh@example.com")
        UserProfile.objects.create(user=sin_rrhh, sucursal=self.sucursal)
        direccion = User.objects.create_superuser(
            username="direccion.alertas", email="direccion@example.com", password="x"
        )
        self.client.force_login(sin_rrhh)

        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(
                reverse("operacion:mermas_insumos_crear_api"),
                data=json.dumps(self.payload()), content_type="application/json",
            )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["estatus"], MermaInsumo.ESTATUS_SIN_RESPONSABLE)
        self.assertEqual(mail.outbox[0].to, [direccion.email])
        self.assertIn("Sin responsable", mail.outbox[0].subject)

    def test_rechaza_codigo_no_elegible_aunque_cliente_lo_envie(self):
        response = self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload(codigo_point="INS-OTRO")),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(MermaInsumo.objects.exists())

    def test_jefe_aprueba_y_crea_orden_simulable_unica(self):
        self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()),
            content_type="application/json",
        )
        merma = MermaInsumo.objects.get()
        self.client.force_login(self.jefe_user)

        response = self.client.post(
            reverse("operacion:mermas_insumos_aprobar_api", args=[merma.id]),
            data=json.dumps({"cantidad": "2.500", "motivo": "Solo 2.5 kg comprobados"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        orden = OrdenAjustePoint.objects.get(merma=merma)
        self.assertEqual(orden.cantidad, Decimal("-2.500"))
        self.assertEqual(orden.estatus, OrdenAjustePoint.ESTATUS_SIMULADA)
        self.assertEqual(orden.existencia_antes, Decimal("8.250"))
        self.assertEqual(orden.existencia_despues, Decimal("5.750"))

    def test_aprobacion_notifica_por_correo_a_reportante(self):
        self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()), content_type="application/json",
        )
        merma = MermaInsumo.objects.get()
        self.client.force_login(self.jefe_user)

        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(
                reverse("operacion:mermas_insumos_aprobar_api", args=[merma.id]),
                data=json.dumps({"cantidad": "3.000", "motivo": ""}), content_type="application/json",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].to, ["encargada@example.com"])
        self.assertIn("Merma aprobada", mail.outbox[0].subject)

    def test_reintento_http_de_aprobacion_devuelve_misma_orden(self):
        self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()), content_type="application/json",
        )
        merma = MermaInsumo.objects.get()
        self.client.force_login(self.jefe_user)
        url = reverse("operacion:mermas_insumos_aprobar_api", args=[merma.id])
        body = json.dumps({"cantidad": "3.000", "motivo": ""})

        first = self.client.post(url, data=body, content_type="application/json")
        second = self.client.post(url, data=body, content_type="application/json")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["orden_id"], second.json()["orden_id"])
        self.assertEqual(OrdenAjustePoint.objects.filter(merma=merma).count(), 1)

    def test_pagina_movil_muestra_capturas_y_bandeja_del_jefe(self):
        response = self.client.get(reverse("operacion:sucursal_tools"))
        self.assertContains(response, "Enviar a Mantenimiento")
        self.assertContains(response, "Enviar a mi jefe")

        self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()), content_type="application/json",
        )
        self.client.force_login(self.jefe_user)
        response = self.client.get(reverse("operacion:sucursal_tools") + "?tab=mermas")
        self.assertContains(response, "Pendientes por aprobar")
        self.assertContains(response, "Fresa fresca")

    def test_post_html_merma_regresa_al_formulario_con_fragmento_estable(self):
        response = self.client.post(reverse("operacion:mermas_insumos_crear_api"), self.payload())

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.endswith("?tab=mermas#merma-form"))

    def test_reportante_reenvia_merma_aclarada_y_requiere_nueva_aprobacion(self):
        self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()), content_type="application/json",
        )
        merma = MermaInsumo.objects.get()
        merma.estatus = MermaInsumo.ESTATUS_EN_ACLARACION
        merma.save(update_fields=["estatus"])

        response = self.client.post(
            reverse("operacion:mermas_insumos_reenviar_api", args=[merma.id]),
            data=json.dumps({
                "cantidad": "2.750", "comentario": "Se volvió a pesar",
                "motivo": "Atiendo la aclaración solicitada",
            }), content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        merma.refresh_from_db()
        self.assertEqual(merma.estatus, MermaInsumo.ESTATUS_ENVIADA)
        self.assertEqual(merma.cantidad_reportada, Decimal("2.750"))
        self.assertFalse(OrdenAjustePoint.objects.filter(merma=merma).exists())

    def test_direccion_reasigna_sin_responsable_desde_api(self):
        self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()), content_type="application/json",
        )
        merma = MermaInsumo.objects.get()
        merma.estatus = MermaInsumo.ESTATUS_SIN_RESPONSABLE
        merma.jefe_empleado = None
        merma.jefe_inmediato = None
        merma.save(update_fields=["estatus", "jefe_empleado", "jefe_inmediato"])
        admin = User.objects.create_superuser(username="direccion", password="x")
        self.client.force_login(admin)

        response = self.client.post(
            reverse("operacion:mermas_insumos_reasignar_api", args=[merma.id]),
            data=json.dumps({"jefe_empleado_id": self.jefe.id, "motivo": "Organigrama validado"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        merma.refresh_from_db()
        self.assertEqual(merma.jefe_inmediato, self.jefe_user)
        self.assertEqual(merma.estatus, MermaInsumo.ESTATUS_ENVIADA)

    def test_direccion_sin_sucursal_abre_bandeja_de_sin_responsable(self):
        self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()), content_type="application/json",
        )
        merma = MermaInsumo.objects.get()
        merma.estatus = MermaInsumo.ESTATUS_SIN_RESPONSABLE
        merma.jefe_empleado = None
        merma.jefe_inmediato = None
        merma.save(update_fields=["estatus", "jefe_empleado", "jefe_inmediato"])
        admin = User.objects.create_superuser(username="direccion.bandeja", password="x")
        self.client.force_login(admin)

        response = self.client.get(reverse("operacion:sucursal_tools") + "?tab=mermas")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Sin responsable asignado")
        self.assertContains(response, "Fresa fresca")

    def test_direccion_sin_sucursal_y_sin_pendientes_abre_bandeja_vacia(self):
        admin = User.objects.create_superuser(username="direccion.vacia", password="x")
        self.client.force_login(admin)

        response = self.client.get(reverse("operacion:sucursal_tools") + "?tab=mermas")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Supervisión de mermas de insumos")
        self.assertNotContains(response, "Enviar a mi jefe")

    def test_aprobacion_html_regresa_a_merma_con_fragmento(self):
        self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()), content_type="application/json",
        )
        merma = MermaInsumo.objects.get()
        self.client.force_login(self.jefe_user)

        response = self.client.post(
            reverse("operacion:mermas_insumos_aprobar_api", args=[merma.id]),
            {"cantidad": "3.000", "motivo": ""},
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.endswith(f"?tab=mermas#merma-{merma.id}"))

    def test_decision_html_invalida_regresa_sin_perder_motivo(self):
        self.client.post(
            reverse("operacion:mermas_insumos_crear_api"),
            data=json.dumps(self.payload()), content_type="application/json",
        )
        merma = MermaInsumo.objects.get()
        self.client.force_login(self.jefe_user)

        response = self.client.post(
            reverse("operacion:mermas_insumos_decidir_api", args=[merma.id]),
            {"accion": "RECHAZAR", "motivo": ""},
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.endswith(f"?tab=mermas#merma-{merma.id}"))

    def test_jefe_puede_pedir_aclaracion_o_rechazar_sin_crear_orden(self):
        for accion, estado in (("ACLARAR", MermaInsumo.ESTATUS_EN_ACLARACION), ("RECHAZAR", MermaInsumo.ESTATUS_RECHAZADA)):
            self.client.force_login(self.user)
            self.client.post(
                reverse("operacion:mermas_insumos_crear_api"),
                data=json.dumps(self.payload()), content_type="application/json",
            )
            merma = MermaInsumo.objects.order_by("-id").first()
            self.client.force_login(self.jefe_user)

            response = self.client.post(
                reverse("operacion:mermas_insumos_decidir_api", args=[merma.id]),
                data=json.dumps({"accion": accion, "motivo": "Se requiere validar evidencia"}),
                content_type="application/json",
            )

            self.assertEqual(response.status_code, 200)
            merma.refresh_from_db()
            self.assertEqual(merma.estatus, estado)
            self.assertFalse(OrdenAjustePoint.objects.filter(merma=merma).exists())
