from datetime import date, timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from rrhh.models import Empleado, IncapacidadCambio, IncapacidadEmpleado


class EdicionIncapacidadesTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="rrhh-incapacidad-edicion",
            email="rrhh-incapacidad-edicion@example.com",
            password="testpass",
        )
        self.empleado = Empleado.objects.create(
            nombre="Empleado Captura Original",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
        )
        self.otro_empleado = Empleado.objects.create(
            nombre="Empleado Correcto",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
        )
        self.incapacidad = IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 7, 1),
            fecha_fin=date(2026, 7, 5),
            tipo=IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
            folio="IMSS-001",
            estado=IncapacidadEmpleado.ESTADO_ACTIVA,
            registrada_por=self.user,
        )
        self.client.force_login(self.user)
        self.url = reverse("rrhh:rrhh_incapacidad_editar", args=[self.incapacidad.id])

    def _payload(self, **overrides):
        payload = {
            "empleado": str(self.empleado.id),
            "fecha_inicio": "2026-07-01",
            "fecha_fin": "2026-07-05",
            "tipo": IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
            "folio": "IMSS-001",
            "estado": IncapacidadEmpleado.ESTADO_ACTIVA,
            "notas": "",
            "motivo": "Corrección de captura",
        }
        payload.update(overrides)
        return payload

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_edicion_guarda_cambios_y_deja_bitacora(self, evaluar):
        self.client.post(
            self.url,
            self._payload(fecha_inicio="2026-07-02", fecha_fin="2026-07-08", folio="IMSS-002"),
        )

        self.incapacidad.refresh_from_db()
        self.assertEqual(self.incapacidad.fecha_inicio, date(2026, 7, 2))
        self.assertEqual(self.incapacidad.fecha_fin, date(2026, 7, 8))
        self.assertEqual(self.incapacidad.folio, "IMSS-002")

        cambio = self.incapacidad.cambios.get()
        self.assertEqual(cambio.accion, IncapacidadCambio.ACCION_EDITAR)
        self.assertEqual(cambio.motivo, "Corrección de captura")
        self.assertEqual(cambio.realizado_por, self.user)
        campos = {detalle["campo"] for detalle in cambio.cambios}
        self.assertEqual(campos, {"Fecha inicio", "Fecha fin", "Folio"})
        inicio = next(d for d in cambio.cambios if d["campo"] == "Fecha inicio")
        self.assertEqual(inicio["antes"], "2026-07-01")
        self.assertEqual(inicio["despues"], "2026-07-02")

        # El rango viejo y el nuevo se reevalúan por separado.
        self.assertEqual(evaluar.call_count, 2)
        self.assertEqual(evaluar.call_args_list[0].args, (date(2026, 7, 1), date(2026, 7, 5)))
        self.assertEqual(evaluar.call_args_list[1].args, (date(2026, 7, 2), date(2026, 7, 8)))

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_cambio_de_empleado_reevalua_ambos(self, evaluar):
        self.client.post(self.url, self._payload(empleado=str(self.otro_empleado.id)))

        self.incapacidad.refresh_from_db()
        self.assertEqual(self.incapacidad.empleado, self.otro_empleado)
        self.assertEqual(evaluar.call_count, 2)
        self.assertEqual(evaluar.call_args_list[0].kwargs["empleados"], [self.empleado])
        self.assertEqual(evaluar.call_args_list[1].kwargs["empleados"], [self.otro_empleado])

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_motivo_obligatorio(self, evaluar):
        self.client.post(self.url, self._payload(fecha_fin="2026-07-09", motivo="  "))

        self.incapacidad.refresh_from_db()
        self.assertEqual(self.incapacidad.fecha_fin, date(2026, 7, 5))
        self.assertFalse(self.incapacidad.cambios.exists())
        evaluar.assert_not_called()

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_rechaza_traslape_con_otra_incapacidad(self, evaluar):
        IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 8, 1),
            fecha_fin=date(2026, 8, 10),
            tipo=IncapacidadEmpleado.TIPO_OTRO,
            estado=IncapacidadEmpleado.ESTADO_ACTIVA,
        )

        self.client.post(self.url, self._payload(fecha_inicio="2026-08-05", fecha_fin="2026-08-06"))

        self.incapacidad.refresh_from_db()
        self.assertEqual(self.incapacidad.fecha_inicio, date(2026, 7, 1))
        self.assertFalse(self.incapacidad.cambios.exists())
        evaluar.assert_not_called()

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_sin_cambios_no_registra_bitacora(self, evaluar):
        self.client.post(self.url, self._payload())

        self.assertFalse(self.incapacidad.cambios.exists())
        evaluar.assert_not_called()

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_reactiva_incapacidad_cancelada(self, evaluar):
        self.incapacidad.estado = IncapacidadEmpleado.ESTADO_CANCELADA
        self.incapacidad.comentario_cancelacion = "Cancelada por error"
        self.incapacidad.save(update_fields=["estado", "comentario_cancelacion"])

        self.client.post(self.url, self._payload(motivo="Se canceló por error"))

        self.incapacidad.refresh_from_db()
        self.assertEqual(self.incapacidad.estado, IncapacidadEmpleado.ESTADO_ACTIVA)
        self.assertEqual(self.incapacidad.comentario_cancelacion, "")
        cambio = self.incapacidad.cambios.get()
        self.assertEqual(
            cambio.cambios,
            [{"campo": "Estado administrativo", "antes": "Cancelada", "despues": "Activa"}],
        )

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_cancelacion_queda_en_la_bitacora(self, evaluar):
        self.client.post(
            reverse("rrhh:rrhh_incapacidad_cancelar", args=[self.incapacidad.id]),
            {"comentario_cancelacion": "Folio duplicado"},
        )

        cambio = self.incapacidad.cambios.get()
        self.assertEqual(cambio.accion, IncapacidadCambio.ACCION_CANCELAR)
        self.assertEqual(cambio.motivo, "Folio duplicado")
        self.assertEqual(
            cambio.cambios,
            [{"campo": "Estado administrativo", "antes": "Activa", "despues": "Cancelada"}],
        )

    def test_listado_enlaza_a_la_pantalla_de_correccion(self):
        response = self.client.get(reverse("rrhh:rrhh_incapacidades"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.url)

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_pantalla_de_correccion_muestra_formulario_y_bitacora(self, evaluar):
        self.client.post(self.url, self._payload(folio="IMSS-003"))

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "rrhh/incapacidad_editar.html")
        self.assertContains(response, "Guardar corrección")
        self.assertContains(response, "Corrección de captura")
        self.assertContains(response, "IMSS-003")

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_rechaza_rango_absurdo_por_error_de_tecleo(self, evaluar):
        self.client.post(self.url, self._payload(fecha_inicio="0202-09-09", fecha_fin="2026-09-10"))

        self.incapacidad.refresh_from_db()
        self.assertEqual(self.incapacidad.fecha_inicio, date(2026, 7, 1))
        self.assertFalse(self.incapacidad.cambios.exists())
        evaluar.assert_not_called()

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_corregir_registro_heredado_acota_la_reevaluacion(self, evaluar):
        # Los registros ya guardados con el año mal tecleado abarcan cientos de
        # miles de días; reevaluarlos completos colgaría la petición.
        heredada = IncapacidadEmpleado.objects.create(
            empleado=self.otro_empleado,
            fecha_inicio=date(202, 9, 9),
            fecha_fin=date(2026, 9, 10),
            tipo=IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
            estado=IncapacidadEmpleado.ESTADO_ACTIVA,
        )

        self.client.post(
            reverse("rrhh:rrhh_incapacidad_editar", args=[heredada.id]),
            {
                "empleado": str(self.otro_empleado.id),
                "fecha_inicio": "2026-09-09",
                "fecha_fin": "2026-09-10",
                "tipo": IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
                "folio": "",
                "estado": IncapacidadEmpleado.ESTADO_ACTIVA,
                "notas": "",
                "motivo": "El año venía mal tecleado",
            },
        )

        heredada.refresh_from_db()
        self.assertEqual(heredada.fecha_inicio, date(2026, 9, 9))
        rango_viejo = evaluar.call_args_list[0].args
        self.assertEqual(
            rango_viejo[0],
            date(2026, 9, 10) - timedelta(days=IncapacidadEmpleado.MAX_DIAS),
        )
        self.assertEqual((rango_viejo[1] - rango_viejo[0]).days, IncapacidadEmpleado.MAX_DIAS)

    def test_usuario_sin_permiso_no_puede_editar(self):
        sin_permiso = get_user_model().objects.create_user(
            username="rrhh-sin-permiso",
            email="rrhh-sin-permiso@example.com",
            password="testpass",
        )
        self.client.force_login(sin_permiso)

        response = self.client.post(self.url, self._payload(fecha_fin="2026-07-09"))

        self.assertEqual(response.status_code, 403)
        self.incapacidad.refresh_from_db()
        self.assertEqual(self.incapacidad.fecha_fin, date(2026, 7, 5))
