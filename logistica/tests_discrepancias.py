from datetime import timedelta
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from logistica.models import DiscrepanciaLogistica, ParadaEntregaEvidencia
from logistica.services_discrepancias import (
    pendientes_vencidos_para_planeacion,
    registrar_discrepancias_recepcion,
    resolver_discrepancia,
)
from logistica.tests_carga_sucursal import PersistenciaCargaSucursalTests
from rrhh.models import Empleado
from core.models import UserModuleAccess


User = get_user_model()


class DiscrepanciasLogisticaTests(PersistenciaCargaSucursalTests):
    def setUp(self):
        super().setUp()
        self.empleado_jefe = Empleado.objects.create(
            codigo="JEFE-DIF",
            nombre="Jefa Ventas",
            departamento=Empleado.DEP_VENTAS,
            usuario_erp=self.jefe,
        )
        Empleado.objects.create(
            codigo="CHOFER-DIF",
            nombre="Chofer Diferencias",
            departamento=Empleado.DEP_LOGISTICA,
            usuario_erp=self.user,
            jefe_directo=self.empleado_jefe,
        )
        self.linea.cantidad_cargada = Decimal("8")
        self.linea.save(update_fields=["cantidad_cargada", "actualizado_en"])

    def test_recepcion_distinta_crea_caso_separado_asignado_al_jefe(self):
        evidencia = ParadaEntregaEvidencia.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            linea_carga=self.linea,
            cantidad_entregada=Decimal("7"),
            comentario="Caja incompleta",
            capturado_por=self.user,
            client_event_id="recepcion-1",
        )

        casos = registrar_discrepancias_recepcion(
            evidencias=[evidencia],
            actor=self.user,
            motivos={self.linea.id: "faltante_fisico"},
        )

        self.assertEqual(len(casos), 1)
        caso = casos[0]
        self.assertEqual(caso.origen, DiscrepanciaLogistica.ORIGEN_RECEPCION)
        self.assertEqual(caso.cantidad_enviada, Decimal("10"))
        self.assertEqual(caso.cantidad_cargada, Decimal("8"))
        self.assertEqual(caso.cantidad_recibida, Decimal("7"))
        self.assertEqual(caso.asignado_a, self.jefe)

    def test_recepcion_distinta_exige_motivo_y_no_crea_caso(self):
        evidencia = ParadaEntregaEvidencia.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            linea_carga=self.linea,
            cantidad_entregada=Decimal("7"),
            capturado_por=self.user,
            client_event_id="recepcion-2",
        )

        with self.assertRaises(ValidationError):
            registrar_discrepancias_recepcion(evidencias=[evidencia], actor=self.user, motivos={})

        self.assertFalse(DiscrepanciaLogistica.objects.filter(origen=DiscrepanciaLogistica.ORIGEN_RECEPCION).exists())

    def test_jefe_resuelve_y_otro_usuario_no_puede(self):
        caso = DiscrepanciaLogistica.objects.create(
            ruta=self.ruta, parada=self.parada, linea_carga=self.linea,
            origen=DiscrepanciaLogistica.ORIGEN_CARGA,
            cantidad_enviada=Decimal("10"), cantidad_cargada=Decimal("8"),
            motivo="faltante_fisico", asignado_a=self.jefe, creado_por=self.user,
        )
        tercero = User.objects.create_user(username="tercero.diferencias")

        with self.assertRaises(PermissionDenied):
            resolver_discrepancia(caso=caso, actor=tercero, accion="validar_real", comentario="No autorizado")
        resolver_discrepancia(caso=caso, actor=self.jefe, accion="solicitar_aclaracion", comentario="Adjuntar evidencia")

        caso.refresh_from_db()
        self.assertEqual(caso.estado, DiscrepanciaLogistica.ESTADO_ACLARACION_SOLICITADA)
        self.assertEqual(caso.revisado_por, self.jefe)

    def test_solo_pendientes_anteriores_bloquean_planeacion(self):
        caso = DiscrepanciaLogistica.objects.create(
            ruta=self.ruta, parada=self.parada, linea_carga=self.linea,
            origen=DiscrepanciaLogistica.ORIGEN_CARGA,
            cantidad_enviada=Decimal("10"), cantidad_cargada=Decimal("8"),
            motivo="faltante_fisico", asignado_a=self.jefe, creado_por=self.user,
        )
        ayer = timezone.now() - timedelta(days=1)
        DiscrepanciaLogistica.objects.filter(pk=caso.pk).update(creado_en=ayer)

        self.assertEqual(list(pendientes_vencidos_para_planeacion(self.jefe, timezone.localdate())), [caso])

    def test_planeacion_muestra_deuda_y_post_no_crea_ruta(self):
        UserModuleAccess.objects.create(user=self.jefe, module="logistica.rutas", access=UserModuleAccess.ACCESS_MANAGE, updated_by=self.jefe)
        caso = DiscrepanciaLogistica.objects.create(
            ruta=self.ruta, parada=self.parada, linea_carga=self.linea,
            origen=DiscrepanciaLogistica.ORIGEN_CARGA,
            cantidad_enviada=Decimal("10"), cantidad_cargada=Decimal("8"),
            motivo="faltante_fisico", asignado_a=self.jefe, creado_por=self.user,
        )
        DiscrepanciaLogistica.objects.filter(pk=caso.pk).update(creado_en=timezone.now() - timedelta(days=1))
        self.client.force_login(self.jefe)
        total_antes = self.ruta.__class__.objects.count()

        get_response = self.client.get(reverse("logistica:rutas"))
        post_response = self.client.post(reverse("logistica:rutas"), {"nombre": "Ruta que no debe crearse"})

        self.assertContains(get_response, "Aclara las diferencias pendientes antes de planear")
        self.assertEqual(post_response.status_code, 403)
        self.assertEqual(self.ruta.__class__.objects.count(), total_antes)

    def test_bandeja_resuelve_discrepancia_con_trazabilidad(self):
        UserModuleAccess.objects.create(user=self.jefe, module="logistica.rutas", access=UserModuleAccess.ACCESS_MANAGE, updated_by=self.jefe)
        caso = DiscrepanciaLogistica.objects.create(
            ruta=self.ruta, parada=self.parada, linea_carga=self.linea,
            origen=DiscrepanciaLogistica.ORIGEN_RECEPCION,
            cantidad_enviada=Decimal("10"), cantidad_cargada=Decimal("8"), cantidad_recibida=Decimal("7"),
            motivo="faltante_fisico", asignado_a=self.jefe, creado_por=self.user,
        )
        self.client.force_login(self.jefe)

        response = self.client.post(reverse("logistica:revisiones_entrega"), {
            "discrepancia_id": caso.id,
            "accion": "validar_real",
            "resolucion": "Diferencia física confirmada por ventas",
        })

        self.assertRedirects(response, reverse("logistica:revisiones_entrega"))
        caso.refresh_from_db()
        self.assertEqual(caso.estado, DiscrepanciaLogistica.ESTADO_VALIDADA_REAL)
