from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import Sucursal
from core.models import AuditLog, Notificacion
from crm.models import Cliente, DireccionCliente, PedidoCliente
from logistica.models import EntregaEcommerce, Repartidor, SolicitudDomicilio
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
