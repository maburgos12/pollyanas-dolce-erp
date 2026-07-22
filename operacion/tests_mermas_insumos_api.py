import json
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.core import mail
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal, UserProfile
from mermas.models import MermaInsumo, OrdenAjustePoint
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob, PointTransferLine
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
