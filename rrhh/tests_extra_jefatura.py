from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.models import Notificacion
from rrhh.bonos_horas_extra import _puede_autorizar_hora_extra
from rrhh.models import AsistenciaEmpleado, Empleado, HoraExtra
from rrhh.services import generar_horas_extra_automatico
from rrhh.services_horas_extra_autorizacion import resolver_hora_extra
from rrhh.services_horas_extra_jefatura import sincronizar_jefe_horas_extra_pendientes


User = get_user_model()


class JefaturaVigenteHorasExtraTests(TestCase):
    def setUp(self):
        self.carolina = User.objects.create_user(username="carolina.test")
        self.johana = User.objects.create_user(username="johana.test")
        self.carolina_empleado = Empleado.objects.create(
            nombre="Carolina", departamento=Empleado.DEP_PRODUCCION,
            nivel_organizacional=Empleado.NIVEL_JEFATURA, usuario_erp=self.carolina,
        )
        self.johana_empleado = Empleado.objects.create(
            nombre="Johana", departamento=Empleado.DEP_VENTAS,
            nivel_organizacional=Empleado.NIVEL_JEFATURA, usuario_erp=self.johana,
        )
        self.empleado = Empleado.objects.create(
            nombre="Sol", departamento=Empleado.DEP_PRODUCCION,
            jefe_directo=self.carolina_empleado, salario_diario=Decimal("400"),
        )
        self.asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado, fecha=date(2026, 9, 18), fuente=AsistenciaEmpleado.FUENTE_POINT,
        )
        self.extra = HoraExtra.objects.create(
            empleado=self.empleado, asistencia=self.asistencia, fecha=self.asistencia.fecha,
            horas=Decimal("0.20"), jefe_directo=self.carolina,
            notas="[Detección automática] Saldo detectado.",
        )

    def trasladar_a_ventas(self, *, con_jefe=True):
        self.empleado.departamento = Empleado.DEP_VENTAS
        self.empleado.jefe_directo = self.johana_empleado if con_jefe else None
        self.empleado.save(update_fields=["departamento", "jefe_directo"])

    def test_jefe_anterior_no_puede_resolver_deteccion_desactualizada(self):
        self.trasladar_a_ventas()
        self.extra.refresh_from_db()
        self.assertFalse(_puede_autorizar_hora_extra(self.carolina, self.extra))
        self.client.force_login(self.carolina)
        bandeja = self.client.get(reverse("rrhh:rrhh_he_list"))
        self.assertEqual(bandeja.status_code, 200)
        self.assertNotContains(bandeja, f'id="hora-extra-{self.extra.pk}"')
        _he, _mensaje, error = resolver_hora_extra(self.extra.pk, "rechazar", self.carolina)
        self.assertIn("jefatura", error)
        self.extra.refresh_from_db()
        self.assertEqual(self.extra.estado, HoraExtra.ESTADO_PENDIENTE)

    def test_api_oculta_y_protege_deteccion_de_jefatura_anterior(self):
        self.trasladar_a_ventas()
        self.client.force_login(self.carolina)
        lista = self.client.get(reverse("rrhh:hora-extra-list"))
        self.assertEqual(lista.status_code, 200)
        self.assertNotIn(self.extra.pk, [item["id"] for item in lista.json()])
        url = reverse("rrhh:hora-extra-detail", args=[self.extra.pk])
        self.assertEqual(self.client.get(url).status_code, 404)
        self.assertEqual(self.client.patch(url, {"notas": "Cambio indebido"}, content_type="application/json").status_code, 404)
        self.assertEqual(self.client.delete(url).status_code, 404)
        self.extra.refresh_from_db()
        self.assertEqual(self.extra.estado, HoraExtra.ESTADO_PENDIENTE)
        self.assertNotEqual(self.extra.notas, "Cambio indebido")

    def test_sincroniza_solo_pendientes_y_traslada_aviso_existente(self):
        aviso = Notificacion.objects.create(
            usuario=self.carolina, tipo=Notificacion.TIPO_HORA_EXTRA,
            titulo="Hora extra pendiente", objeto_tipo="rrhh.HoraExtra", objeto_id=str(self.extra.pk),
        )
        historica = HoraExtra.objects.create(
            empleado=self.empleado, fecha=date(2026, 9, 17), horas=Decimal("1"),
            estado=HoraExtra.ESTADO_AUTORIZADO, jefe_directo=self.carolina,
        )
        self.trasladar_a_ventas()
        self.assertEqual(sincronizar_jefe_horas_extra_pendientes(self.empleado), 1)
        self.assertEqual(sincronizar_jefe_horas_extra_pendientes(self.empleado), 0)
        self.extra.refresh_from_db()
        historica.refresh_from_db()
        aviso.refresh_from_db()
        self.assertEqual(self.extra.jefe_directo, self.johana)
        self.assertEqual(self.extra.horas, Decimal("0.20"))
        self.assertEqual(self.extra.estado, HoraExtra.ESTADO_PENDIENTE)
        self.assertEqual(historica.jefe_directo, self.carolina)
        self.assertTrue(aviso.leida)
        self.assertTrue(Notificacion.objects.filter(
            usuario=self.johana, objeto_tipo="rrhh.HoraExtra", objeto_id=str(self.extra.pk), leida=False,
        ).exists())

    def test_deteccion_sin_aviso_previo_no_crea_notificacion_nueva(self):
        self.trasladar_a_ventas()
        self.assertEqual(sincronizar_jefe_horas_extra_pendientes(self.empleado), 1)
        self.assertFalse(Notificacion.objects.filter(objeto_id=str(self.extra.pk)).exists())

    def test_reevaluacion_corrige_jefe_aunque_no_haya_turno(self):
        self.trasladar_a_ventas(con_jefe=False)
        generar_horas_extra_automatico(self.asistencia)
        self.extra.refresh_from_db()
        self.assertIsNone(self.extra.jefe_directo_id)
        self.assertEqual(self.extra.estado, HoraExtra.ESTADO_PENDIENTE)

    def test_editar_ficha_reasigna_deteccion_pendiente(self):
        admin = User.objects.create_superuser(username="admin.jefatura", email="admin@example.com", password="x")
        self.client.force_login(admin)
        response = self.client.post(reverse("rrhh:empleados"), {
            "action": "update", "empleado_id": str(self.empleado.pk), "nombre": self.empleado.nombre,
            "codigo": self.empleado.codigo, "salario_diario": "400.00", "activo": "on",
            "area": "CAJAS", "departamento_origen": "VENTAS", "departamento": "VENTAS",
            "puesto_operativo": "CAJAS", "jefe_directo": str(self.johana_empleado.pk),
        })
        self.assertEqual(response.status_code, 302)
        self.empleado.refresh_from_db()
        self.extra.refresh_from_db()
        self.assertEqual(self.empleado.jefe_directo, self.johana_empleado)
        self.assertEqual(self.extra.jefe_directo, self.johana)
        self.client.force_login(self.carolina)
        anterior = self.client.get(reverse("rrhh:rrhh_he_list"))
        self.assertEqual(anterior.status_code, 403)
        self.client.force_login(self.johana)
        vigente = self.client.get(reverse("rrhh:rrhh_he_list"))
        self.assertEqual(vigente.status_code, 200)
        self.assertContains(vigente, f'id="hora-extra-{self.extra.pk}"')
