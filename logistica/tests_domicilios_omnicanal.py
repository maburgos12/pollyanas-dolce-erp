from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import Sucursal
from crm.models import Cliente, DireccionCliente, PedidoCliente
from logistica.models import Repartidor, SolicitudDomicilio


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
