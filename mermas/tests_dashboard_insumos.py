from datetime import date
from decimal import Decimal

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal, UserModuleAccess, UserProfile
from maestros.models import CostoInsumo, Insumo
from mermas.models import MermaInsumo
from mermas.services_insumos import enviar_merma_insumo
from rrhh.models import Empleado


class MermaInsumoCostoHistoricoTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán")
        self.usuario = User.objects.create_user(username="captura.costo")
        self.jefe_user = User.objects.create_user(username="jefa.costo")
        jefe = Empleado.objects.create(
            nombre="Jefa costo", usuario_erp=self.jefe_user, sucursal_ref=self.sucursal
        )
        Empleado.objects.create(
            nombre="Captura costo", usuario_erp=self.usuario, sucursal_ref=self.sucursal, jefe_directo=jefe
        )
        self.insumo = Insumo.objects.create(nombre="Fresa", codigo_point="INS-COSTO")

    def test_envio_congela_ultimo_costo_no_posterior(self):
        anterior = CostoInsumo.objects.create(
            insumo=self.insumo, fecha=date(2026, 1, 1), costo_unitario=Decimal("40"),
            moneda="MXN", source_hash="costo-anterior",
        )
        CostoInsumo.objects.create(
            insumo=self.insumo, fecha=date(2099, 1, 1), costo_unitario=Decimal("99"),
            moneda="MXN", source_hash="costo-futuro",
        )
        merma = MermaInsumo.objects.create(
            sucursal=self.sucursal, reportado_por=self.usuario,
            codigo_point="INS-COSTO", nombre_point="Fresa", unidad_point="KG",
            cantidad_reportada=Decimal("2"), motivo="CALIDAD", comentario="No apta",
            justificacion_sin_foto="Sin cámara",
        )

        enviada = enviar_merma_insumo(merma_id=merma.id, usuario=self.usuario)

        self.assertEqual(enviada.costo_unitario_historico, Decimal("40"))
        self.assertEqual(enviada.insumo, self.insumo)
        self.assertEqual(enviada.costo_fuente_id, str(anterior.id))
        self.assertEqual(enviada.estado_valorizacion, MermaInsumo.VALORIZACION_CON_COSTO)

    def test_envio_sin_costo_no_bloquea_y_marca_revision(self):
        merma = MermaInsumo.objects.create(
            sucursal=self.sucursal, reportado_por=self.usuario, insumo=self.insumo,
            codigo_point="INS-COSTO", nombre_point="Fresa", unidad_point="KG",
            cantidad_reportada=Decimal("2"), motivo="CALIDAD", comentario="No apta",
            justificacion_sin_foto="Sin cámara",
        )

        enviada = enviar_merma_insumo(merma_id=merma.id, usuario=self.usuario)

        self.assertEqual(enviada.estado_valorizacion, MermaInsumo.VALORIZACION_SIN_COSTO)
        self.assertIsNone(enviada.costo_unitario_historico)


class MermaInsumoDashboardTests(TestCase):
    def setUp(self):
        self.admin = User.objects.create_superuser("direccion.mermas", "d@example.com", "x")
        self.sucursal = Sucursal.objects.create(codigo="CENTRO", nombre="Centro")
        self.insumo = Insumo.objects.create(nombre="Crema", codigo_point="INS-2")
        self.merma = MermaInsumo.objects.create(
            sucursal=self.sucursal, reportado_por=self.admin, insumo=self.insumo,
            codigo_point="INS-2", nombre_point="Crema", unidad_point="KG",
            cantidad_reportada=Decimal("3"), cantidad_aprobada=Decimal("2"),
            costo_unitario_historico=Decimal("50"), costo_moneda="MXN",
            estado_valorizacion=MermaInsumo.VALORIZACION_CON_COSTO,
            motivo="CADUCIDAD", comentario="Caducó", justificacion_sin_foto="Sin cámara",
            estatus=MermaInsumo.ESTATUS_APROBADA,
        )
        self.client.force_login(self.admin)

    def test_pestana_insumos_muestra_costo_confirmado(self):
        response = self.client.get(reverse("mermas:dashboard"), {"tab": "insumos"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Mermas de insumos")
        self.assertContains(response, "$100.00")
        self.assertContains(response, "Crema")

    def test_exportacion_excel_respeta_dashboard(self):
        response = self.client.get(reverse("mermas:exportar_insumos"), {"sucursal": self.sucursal.id})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response["Content-Type"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        self.assertGreater(len(response.content), 1000)

    def test_usuario_con_acceso_consulta_solo_su_sucursal(self):
        otra_sucursal = Sucursal.objects.create(codigo="NORTE", nombre="Norte")
        MermaInsumo.objects.create(
            sucursal=otra_sucursal, reportado_por=self.admin, insumo=self.insumo,
            codigo_point="INS-2", nombre_point="Crema ajena", unidad_point="KG",
            cantidad_reportada=Decimal("5"), cantidad_aprobada=Decimal("5"),
            costo_unitario_historico=Decimal("50"), costo_moneda="MXN",
            estado_valorizacion=MermaInsumo.VALORIZACION_CON_COSTO,
            motivo="CADUCIDAD", comentario="Ajena", justificacion_sin_foto="Sin cámara",
            estatus=MermaInsumo.ESTATUS_APROBADA,
        )
        jefa = User.objects.create_user("jefa.centro", password="x")
        UserProfile.objects.create(user=jefa, sucursal=self.sucursal)
        UserModuleAccess.objects.create(
            user=jefa, module="mermas.dashboard", access=UserModuleAccess.ACCESS_VIEW
        )
        self.client.force_login(jefa)

        response = self.client.get(reverse("mermas:dashboard"), {"tab": "insumos"})

        self.assertContains(response, "Crema")
        self.assertNotContains(response, "Crema ajena")
        self.assertContains(response, "$100.00")

        detail = self.client.get(reverse("mermas:detalle_insumo", args=[self.merma.id + 1]))
        self.assertEqual(detail.status_code, 404)
