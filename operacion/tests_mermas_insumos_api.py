import json
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
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
        self.user = User.objects.create_user(username="encargada.payan")
        self.jefe_user = User.objects.create_user(username="jefe.payan")
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
