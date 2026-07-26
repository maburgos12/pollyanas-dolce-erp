from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import Sucursal
from core.models import AuditLog, Notificacion
from crm.models import Cliente, DireccionCliente, PedidoCliente
from logistica.models import (
    EntregaEcommerce,
    Repartidor,
    SolicitudDomicilio,
    Unidad,
)
from logistica.services_domicilio_assignment import assign_domicilio
from rrhh.models import Empleado


class SolicitudDomicilioOmnicanalTests(APITestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="DOM-TEST", nombre="Domicilios Test")
        self.user = User.objects.create_user(username="repartidor_domicilios", password="pass123")
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal)

    def test_solicitud_ligada_conserva_cliente_direccion_y_texto_historico(self):
        cliente = Cliente.objects.create(nombre="Ana Pérez", telefono="6671234567")
        direccion = DireccionCliente.objects.create(
            cliente=cliente,
            alias="Casa",
            direccion="Av. Obregón 123",
            referencias="Portón blanco",
            latitud="24.809064",
            longitud="-107.394011",
        )
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            direccion_entrega=direccion,
            descripcion="Pedido con domicilio",
        )

        solicitud = SolicitudDomicilio.objects.create(
            cliente=cliente,
            direccion_cliente=direccion,
            pedido_cliente=pedido,
            cliente_nombre=cliente.nombre,
            cliente_telefono=cliente.telefono,
            direccion=direccion.direccion,
            repartidor=self.repartidor,
            estatus=SolicitudDomicilio.ESTATUS_ASIGNADO,
        )

        self.assertEqual(solicitud.cliente_id, cliente.id)
        self.assertEqual(solicitud.direccion_cliente_id, direccion.id)
        self.assertEqual(solicitud.cliente_nombre, "Ana Pérez")
        self.assertEqual(solicitud.direccion, "Av. Obregón 123")

    def test_api_repartidor_mantiene_fallback_de_solicitud_historica(self):
        SolicitudDomicilio.objects.create(
            cliente_nombre="Cliente legado",
            cliente_telefono="6670000000",
            direccion="Dirección histórica 45",
            notas="Tocar timbre",
            repartidor=self.repartidor,
            estatus=SolicitudDomicilio.ESTATUS_ASIGNADO,
        )
        self.client.force_authenticate(self.user)

        response = self.client.get(reverse("api_logistica_domicilios_generales_asignados"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]["cliente_nombre"], "Cliente legado")
        self.assertEqual(response.data[0]["direccion"], "Dirección histórica 45")
        self.assertEqual(response.data[0]["notas"], "Tocar timbre")


class AsignacionDomicilioApiTests(APITestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="DOM-ASIG", nombre="Domicilios Asignación")
        self.manager = User.objects.create_superuser(
            username="manager_domicilios", email="manager@example.com", password="pass123"
        )
        self.client.force_authenticate(self.manager)
        self.solicitud = SolicitudDomicilio.objects.create(
            cliente_nombre="Cliente asignable",
            direccion="Calle 10",
        )

    def _repartidor(self, username, **kwargs):
        user = User.objects.create_user(username=username, password="pass123")
        return Repartidor.objects.create(user=user, sucursal=self.sucursal, **kwargs)

    def test_catalogo_solo_devuelve_repartidores_activos_y_autorizados(self):
        disponible = self._repartidor("rep_disponible")
        inactivo = self._repartidor("rep_inactivo")
        inactivo.user.is_active = False
        inactivo.user.save(update_fields=["is_active"])
        baja_rrhh = self._repartidor("rep_baja")
        Empleado.objects.create(
            codigo="EMP-BAJA",
            nombre="Repartidor baja",
            usuario_erp=baja_rrhh.user,
            activo=False,
        )
        self._repartidor(
            "rep_tecnico",
            tipo_identidad=Repartidor.TIPO_CUENTA_TECNICA,
        )

        response = self.client.get(reverse("api_logistica_repartidores_disponibles"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["id"] for item in response.data], [disponible.id])
        rejected = self.client.post(
            reverse("api_logistica_domicilio_asignar", args=[self.solicitud.id]),
            {"repartidor_id": inactivo.id},
            format="json",
        )
        self.assertEqual(rejected.status_code, status.HTTP_400_BAD_REQUEST)
        self.solicitud.refresh_from_db()
        self.assertIsNone(self.solicitud.repartidor_id)

    def test_asignacion_idempotente_y_reasignacion_auditada(self):
        primero = self._repartidor("rep_primero")
        segundo = self._repartidor("rep_segundo")
        url = reverse("api_logistica_domicilio_asignar", args=[self.solicitud.id])

        first = self.client.post(url, {"repartidor_id": primero.id}, format="json")
        repeated = self.client.post(url, {"repartidor_id": primero.id}, format="json")
        reassigned = self.client.post(url, {"repartidor_id": segundo.id}, format="json")

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(repeated.status_code, status.HTTP_200_OK)
        self.assertTrue(repeated.data["idempotent"])
        self.assertEqual(reassigned.status_code, status.HTTP_200_OK)
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.repartidor_id, segundo.id)
        logs = AuditLog.objects.filter(
            model="logistica.SolicitudDomicilio",
            object_id=str(self.solicitud.id),
            action="ASSIGN",
        ).order_by("timestamp")
        self.assertEqual(logs.count(), 2)
        self.assertEqual(logs.last().payload["repartidor_anterior_id"], primero.id)
        self.assertEqual(logs.last().payload["repartidor_nuevo_id"], segundo.id)
        self.assertEqual(logs.last().user_id, self.manager.id)
        self.assertEqual(EntregaEcommerce.objects.count(), 0)
        self.assertEqual(Notificacion.objects.count(), 0)

    def test_repeticion_idempotente_conserva_estado_terminal_sin_efectos(self):
        repartidor = self._repartidor("rep_entregado")
        reemplazo = self._repartidor("rep_reemplazo_terminal")
        self.solicitud.repartidor = repartidor
        self.solicitud.estatus = SolicitudDomicilio.ESTATUS_ENTREGADO
        self.solicitud.save(update_fields=["repartidor", "estatus"])
        url = reverse("api_logistica_domicilio_asignar", args=[self.solicitud.id])

        response = self.client.post(
            url,
            {"repartidor_id": repartidor.id},
            format="json",
        )
        cambio_real = self.client.post(
            url,
            {"repartidor_id": reemplazo.id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["idempotent"])
        self.assertEqual(cambio_real.status_code, status.HTTP_409_CONFLICT)
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.estatus, SolicitudDomicilio.ESTATUS_ENTREGADO)
        self.assertEqual(self.solicitud.repartidor_id, repartidor.id)
        self.assertEqual(AuditLog.objects.count(), 0)
        self.assertEqual(EntregaEcommerce.objects.count(), 0)
        self.assertEqual(Notificacion.objects.count(), 0)

    def test_repeticion_idempotente_acepta_repartidor_que_quedo_inactivo(self):
        repartidor = self._repartidor("rep_asignado_inactivo")
        self.solicitud.repartidor = repartidor
        self.solicitud.estatus = SolicitudDomicilio.ESTATUS_CANCELADO
        self.solicitud.save(update_fields=["repartidor", "estatus"])
        repartidor.user.is_active = False
        repartidor.user.save(update_fields=["is_active"])
        url = reverse("api_logistica_domicilio_asignar", args=[self.solicitud.id])

        response = self.client.post(
            url,
            {"repartidor_id": repartidor.id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["idempotent"])
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.estatus, SolicitudDomicilio.ESTATUS_CANCELADO)
        self.assertEqual(self.solicitud.repartidor_id, repartidor.id)
        self.assertEqual(AuditLog.objects.count(), 0)
        self.assertEqual(EntregaEcommerce.objects.count(), 0)
        self.assertEqual(Notificacion.objects.count(), 0)

    def test_rechaza_sin_permisos_y_repartidor_no_disponible(self):
        repartidor = self._repartidor("rep_no_disponible")
        repartidor.user.is_active = False
        repartidor.user.save(update_fields=["is_active"])
        user = User.objects.create_user(username="sin_permiso", password="pass123")
        self.client.force_authenticate(user)

        catalog = self.client.get(reverse("api_logistica_repartidores_disponibles"))
        assign = self.client.post(
            reverse("api_logistica_domicilio_asignar", args=[self.solicitud.id]),
            {"repartidor_id": repartidor.id},
            format="json",
        )

        self.assertEqual(catalog.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(assign.status_code, status.HTTP_403_FORBIDDEN)

    def test_asignacion_html_delega_servicio_revision_unidad_y_auditoria(self):
        self.client.force_login(self.manager)
        repartidor = self._repartidor("rep_html")
        unidad = Unidad.objects.create(
            codigo="DOM-HTML",
            descripcion="Unidad HTML",
            sucursal=self.sucursal,
        )

        response = self.client.post(
            reverse("logistica:domicilios_generales"),
            {
                "accion": "asignar",
                "solicitud_id": self.solicitud.id,
                "repartidor": repartidor.id,
                "unidad_operativa": unidad.id,
            },
        )

        self.assertRedirects(
            response,
            reverse("logistica:domicilios_generales"),
            fetch_redirect_response=False,
        )
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.repartidor_id, repartidor.id)
        self.assertEqual(self.solicitud.unidad_id, unidad.id)
        self.assertEqual(self.solicitud.estatus, SolicitudDomicilio.ESTATUS_ASIGNADO)
        self.assertEqual(self.solicitud.revision, 1)
        audit = AuditLog.objects.get(
            model="logistica.SolicitudDomicilio",
            object_id=str(self.solicitud.id),
            action="ASSIGN",
        )
        self.assertEqual(audit.user_id, self.manager.id)
        self.assertEqual(audit.payload["unidad"], unidad.codigo)

    def test_asignacion_html_no_resetea_terminal(self):
        self.client.force_login(self.manager)
        actual = self._repartidor("rep_html_terminal")
        reemplazo = self._repartidor("rep_html_reemplazo")
        unidad = Unidad.objects.create(
            codigo="DOM-TERM",
            descripcion="Unidad terminal",
            sucursal=self.sucursal,
        )
        self.solicitud.repartidor = actual
        self.solicitud.unidad = unidad
        self.solicitud.estatus = SolicitudDomicilio.ESTATUS_ENTREGADO
        self.solicitud.revision = 7
        self.solicitud.save(
            update_fields=["repartidor", "unidad", "estatus", "revision"]
        )

        response = self.client.post(
            reverse("logistica:domicilios_generales"),
            {
                "accion": "asignar",
                "solicitud_id": self.solicitud.id,
                "repartidor": reemplazo.id,
                "unidad_operativa": unidad.id,
            },
        )

        self.assertRedirects(
            response,
            reverse("logistica:domicilios_generales"),
            fetch_redirect_response=False,
        )
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.repartidor_id, actual.id)
        self.assertEqual(self.solicitud.estatus, SolicitudDomicilio.ESTATUS_ENTREGADO)
        self.assertEqual(self.solicitud.revision, 7)
        self.assertEqual(AuditLog.objects.count(), 0)

    def test_html_no_ofrece_ni_acepta_tecnico_o_baja_rrhh(self):
        self.client.force_login(self.manager)
        disponible = self._repartidor("rep_html_disponible")
        tecnico = self._repartidor(
            "rep_html_tecnico",
            tipo_identidad=Repartidor.TIPO_CUENTA_TECNICA,
        )
        baja = self._repartidor("rep_html_baja")
        Empleado.objects.create(
            codigo="EMP-HTML-BAJA",
            nombre="Baja HTML",
            usuario_erp=baja.user,
            activo=False,
        )
        unidad = Unidad.objects.create(
            codigo="DOM-FILTER",
            descripcion="Unidad filtro",
            sucursal=self.sucursal,
        )

        page = self.client.get(reverse("logistica:domicilios_generales"))

        repartidor_ids = {
            item.id for item in page.context["repartidores"]
        }
        self.assertIn(disponible.id, repartidor_ids)
        self.assertNotIn(tecnico.id, repartidor_ids)
        self.assertNotIn(baja.id, repartidor_ids)
        rejected = self.client.post(
            reverse("logistica:domicilios_generales"),
            {
                "accion": "asignar",
                "solicitud_id": self.solicitud.id,
                "repartidor": tecnico.id,
                "unidad_operativa": unidad.id,
            },
        )
        self.assertEqual(rejected.status_code, status.HTTP_302_FOUND)
        self.solicitud.refresh_from_db()
        self.assertIsNone(self.solicitud.repartidor_id)
        self.assertEqual(AuditLog.objects.count(), 0)

    def test_cambio_solo_unidad_audita_snapshot_canonico_y_retry_no_audita(self):
        repartidor = self._repartidor("rep_unit_only")
        anterior = Unidad.objects.create(
            codigo="DOM-U1",
            descripcion="Unidad anterior",
            sucursal=self.sucursal,
        )
        nueva = Unidad.objects.create(
            codigo="DOM-U2",
            descripcion="Unidad nueva",
            sucursal=self.sucursal,
        )
        self.solicitud.repartidor = repartidor
        self.solicitud.unidad = anterior
        self.solicitud.estatus = SolicitudDomicilio.ESTATUS_ASIGNADO
        self.solicitud.save(update_fields=["repartidor", "unidad", "estatus"])

        changed = assign_domicilio(
            solicitud_id=self.solicitud.id,
            repartidor_id=repartidor.id,
            unidad=nueva,
            audit_user=self.manager,
            audit_metadata={
                "unidad_anterior_id": 999999,
                "unidad_nueva_codigo": "FALSO",
            },
        )
        retried = assign_domicilio(
            solicitud_id=self.solicitud.id,
            repartidor_id=repartidor.id,
            unidad=nueva,
            audit_user=self.manager,
        )

        self.assertFalse(changed["idempotent"])
        self.assertTrue(retried["idempotent"])
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.unidad_id, nueva.id)
        self.assertEqual(self.solicitud.revision, 1)
        audit = AuditLog.objects.get(action="ASSIGN")
        self.assertEqual(audit.payload["unidad_anterior_id"], anterior.id)
        self.assertEqual(audit.payload["unidad_anterior_codigo"], anterior.codigo)
        self.assertEqual(audit.payload["unidad_nueva_id"], nueva.id)
        self.assertEqual(audit.payload["unidad_nueva_codigo"], nueva.codigo)
        self.assertEqual(AuditLog.objects.count(), 1)
