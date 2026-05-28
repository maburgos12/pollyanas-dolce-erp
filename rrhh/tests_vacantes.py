from datetime import date

from django.contrib.auth.models import Group, User
from django.core.exceptions import PermissionDenied
from django.test import TestCase
from django.urls import reverse

from core.models import Notificacion, Sucursal
from rrhh.models import Empleado, VacanteCobertura, VacanteMovimiento, VacanteRRHH
from rrhh.services_vacantes import (
    aprobar_vacante_direccion,
    cubrir_vacante,
    crear_solicitud_vacante,
    enviar_vacante_direccion,
    iniciar_reclutamiento_vacante,
)


class VacantesSolicitudServiceTests(TestCase):
    def setUp(self):
        self.rrhh_user = User.objects.create_user(username="paula.rrhh", password="pass123")
        self.rrhh_user.groups.add(Group.objects.get_or_create(name="RRHH")[0])
        self.dg_user = User.objects.create_user(username="mauricio.dg", password="pass123")
        self.dg_user.groups.add(Group.objects.get_or_create(name="DG")[0])
        self.solicitante = User.objects.create_user(username="johana.ventas", password="pass123")
        self.solicitante.groups.add(Group.objects.get_or_create(name="VENTAS")[0])
        self.sucursal = Sucursal.objects.create(codigo="GSV", nombre="Guasave Centro")

    def test_crear_solicitud_vacante_genera_folio_historial_y_notifica_rrhh(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            sucursal=self.sucursal,
            departamento=Empleado.DEP_VENTAS,
            cantidad_solicitada=2,
            motivo_solicitud="Cubrir dos cajas por apertura de turno.",
        )

        self.assertTrue(vacante.folio.startswith("VAC-2605-"))
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_SOLICITADA)
        self.assertEqual(vacante.area, "VENTAS")
        self.assertEqual(vacante.puesto, "CAJERA")
        self.assertEqual(vacante.cantidad_solicitada, 2)
        self.assertEqual(vacante.solicitado_por, self.solicitante)
        self.assertEqual(vacante.movimientos.count(), 1)
        self.assertEqual(vacante.movimientos.first().estado_nuevo, VacanteRRHH.ESTADO_SOLICITADA)
        self.assertTrue(
            Notificacion.objects.filter(
                usuario=self.rrhh_user,
                objeto_tipo="rrhh.VacanteRRHH",
                objeto_id=str(vacante.id),
            ).exists()
        )

    def test_flujo_direccion_reclutamiento_y_cobertura_cierra_solicitud(self):
        vacante = crear_solicitud_vacante(
            area="logistica",
            puesto="repartidor",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            cantidad_solicitada=1,
        )

        enviar_vacante_direccion(vacante, self.rrhh_user, "Validada por CH")
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_PENDIENTE_DIRECCION)
        self.assertEqual(vacante.validado_rrhh_por, self.rrhh_user)
        self.assertTrue(Notificacion.objects.filter(usuario=self.dg_user, objeto_id=str(vacante.id)).exists())

        aprobar_vacante_direccion(vacante, self.dg_user, "Aprobada por Dirección")
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_AUTORIZADA)
        self.assertEqual(vacante.autorizado_por, self.dg_user)

        iniciar_reclutamiento_vacante(vacante, self.rrhh_user)
        empleado = Empleado.objects.create(nombre="Repartidor Nuevo", departamento=Empleado.DEP_LOGISTICA)
        cobertura = cubrir_vacante(vacante, empleado, self.rrhh_user, fecha_cobertura=date(2026, 6, 3))
        vacante.refresh_from_db()

        self.assertEqual(cobertura.vacante, vacante)
        self.assertEqual(VacanteCobertura.objects.filter(vacante=vacante).count(), 1)
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_CUBIERTA)
        self.assertEqual(vacante.fecha_cubierta, date(2026, 6, 3))
        self.assertEqual(vacante.empleado_cubrio, empleado)
        self.assertEqual(VacanteMovimiento.objects.filter(vacante=vacante).count(), 5)

    def test_direccion_no_puede_aprobar_su_propia_solicitud(self):
        vacante = crear_solicitud_vacante(
            area="administracion",
            puesto="analista",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.dg_user,
            creado_por=self.dg_user,
        )
        enviar_vacante_direccion(vacante, self.rrhh_user)

        with self.assertRaises(PermissionDenied):
            aprobar_vacante_direccion(vacante, self.dg_user)


class VacantesSolicitudViewTests(TestCase):
    def setUp(self):
        self.rrhh_user = User.objects.create_user(username="paula.rrhh", password="pass123")
        self.rrhh_user.groups.add(Group.objects.get_or_create(name="RRHH")[0])
        self.dg_user = User.objects.create_user(username="mauricio.dg", password="pass123")
        self.dg_user.groups.add(Group.objects.get_or_create(name="DG")[0])
        self.sucursal = Sucursal.objects.create(codigo="MTR", nombre="Matriz")

    def test_formulario_crea_solicitud_y_redirige_al_detalle(self):
        self.client.force_login(self.rrhh_user)

        response = self.client.post(
            reverse("rrhh:rrhh_vacante_nueva"),
            {
                "area": "produccion",
                "puesto": "hornero",
                "departamento": Empleado.DEP_PRODUCCION,
                "sucursal": self.sucursal.id,
                "fecha_solicitada": "2026-05-28",
                "fecha_necesaria": "2026-06-05",
                "cantidad_solicitada": "1",
                "tipo_solicitud": VacanteRRHH.TIPO_REEMPLAZO,
                "prioridad": VacanteRRHH.PRIORIDAD_ALTA,
                "motivo_solicitud": "Reposición por baja operativa.",
            },
        )

        vacante = VacanteRRHH.objects.get()
        self.assertRedirects(response, reverse("rrhh:rrhh_vacante_detalle", kwargs={"pk": vacante.pk}))
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_SOLICITADA)
        self.assertEqual(vacante.solicitado_por, self.rrhh_user)

    def test_detalle_ejecuta_acciones_de_solicitud(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.rrhh_user,
            creado_por=self.rrhh_user,
        )
        self.client.force_login(self.rrhh_user)

        response = self.client.post(
            reverse("rrhh:rrhh_vacante_accion", kwargs={"pk": vacante.pk}),
            {"action": "enviar_direccion", "comentario": "Lista para aprobar"},
        )
        self.assertRedirects(response, reverse("rrhh:rrhh_vacante_detalle", kwargs={"pk": vacante.pk}))
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_PENDIENTE_DIRECCION)

        self.client.force_login(self.dg_user)
        response = self.client.post(
            reverse("rrhh:rrhh_vacante_accion", kwargs={"pk": vacante.pk}),
            {"action": "aprobar", "comentario": "Aprobada"},
        )
        self.assertRedirects(response, reverse("rrhh:rrhh_vacante_detalle", kwargs={"pk": vacante.pk}))
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_AUTORIZADA)
