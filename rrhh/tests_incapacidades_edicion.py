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

    def _dias_evaluados(self, evaluar):
        """Días que el motor reevaluó, por empleado."""
        por_empleado = {}
        for llamada in evaluar.call_args_list:
            inicio, fin = llamada.args
            self.assertEqual(inicio, fin, "se evalúa día por día, no por rangos")
            for empleado in llamada.kwargs["empleados"]:
                por_empleado.setdefault(empleado.id, set()).add(inicio)
        return por_empleado

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

        # Solo los días que cambiaron de cobertura: sale el 1, entran 6, 7 y 8.
        # Del 2 al 5 siguen cubiertos y no se tocan.
        self.assertEqual(
            self._dias_evaluados(evaluar),
            {
                self.empleado.id: {
                    date(2026, 7, 1),
                    date(2026, 7, 6),
                    date(2026, 7, 7),
                    date(2026, 7, 8),
                }
            },
        )

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_cambio_de_empleado_reevalua_ambos(self, evaluar):
        self.client.post(self.url, self._payload(empleado=str(self.otro_empleado.id)))

        self.incapacidad.refresh_from_db()
        self.assertEqual(self.incapacidad.empleado, self.otro_empleado)
        # Los cinco días dejan de cubrir a uno y pasan a cubrir al otro.
        rango = {date(2026, 7, d) for d in range(1, 6)}
        self.assertEqual(
            self._dias_evaluados(evaluar),
            {self.empleado.id: rango, self.otro_empleado.id: rango},
        )

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
        dias = self._dias_evaluados(evaluar)[self.otro_empleado.id]
        # El tope acota cuánto se camina hacia atrás...
        self.assertEqual(min(dias), date(2026, 9, 10) - timedelta(days=IncapacidadEmpleado.MAX_DIAS))
        # ...y los días que siguen cubiertos no se vuelven a evaluar.
        self.assertNotIn(date(2026, 9, 9), dias)
        self.assertNotIn(date(2026, 9, 10), dias)

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_folio_repetido_nombra_el_registro_en_conflicto(self, evaluar):
        # Caso real: se intenta completar el registro cancelado con el folio que ya
        # tiene el activo. Debe decir cuál es, no el nombre interno de la restricción.
        cancelada = IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 9, 9),
            fecha_fin=date(2026, 9, 10),
            tipo=IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
            estado=IncapacidadEmpleado.ESTADO_CANCELADA,
        )
        activa = IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 9, 8),
            fecha_fin=date(2026, 9, 10),
            tipo=IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
            folio="GC829675",
            estado=IncapacidadEmpleado.ESTADO_ACTIVA,
        )

        response = self.client.post(
            reverse("rrhh:rrhh_incapacidad_editar", args=[cancelada.id]),
            {
                "empleado": str(self.empleado.id),
                "fecha_inicio": "2026-09-09",
                "fecha_fin": "2026-09-10",
                "tipo": IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
                "folio": "GC829675",
                "estado": IncapacidadEmpleado.ESTADO_ACTIVA,
                "notas": "",
                "motivo": "Le falta el folio",
            },
            follow=True,
        )

        avisos = [str(m) for m in response.context["messages"]]
        self.assertTrue(avisos)
        texto = " ".join(avisos)
        self.assertNotIn("rrhh_incapacidad_folio_unico_empleado", texto)
        self.assertNotIn("restricción", texto)
        self.assertIn(f"#{activa.id}", texto)
        self.assertIn("GC829675", texto)
        cancelada.refresh_from_db()
        self.assertEqual(cancelada.folio, "")

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_traslape_nombra_el_registro_en_conflicto(self, evaluar):
        otra = IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 8, 1),
            fecha_fin=date(2026, 8, 10),
            tipo=IncapacidadEmpleado.TIPO_OTRO,
            estado=IncapacidadEmpleado.ESTADO_ACTIVA,
        )

        response = self.client.post(
            self.url,
            self._payload(fecha_inicio="2026-08-05", fecha_fin="2026-08-06"),
            follow=True,
        )

        texto = " ".join(str(m) for m in response.context["messages"])
        self.assertIn(f"#{otra.id}", texto)
        self.assertIn("2026-08-01", texto)

    def test_pantallas_toleran_usuario_borrado(self):
        # registrada_por / realizado_por son SET_NULL: la plantilla no puede reventar.
        self.incapacidad.registrada_por = None
        self.incapacidad.save(update_fields=["registrada_por"])

        self.assertEqual(self.client.get(reverse("rrhh:rrhh_incapacidades")).status_code, 200)
        self.assertEqual(self.client.get(self.url).status_code, 200)

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_incapacidad_larga_solo_reevalua_los_dias_agregados(self, evaluar):
        # Enfermedad o accidente de meses: extender la fecha final no puede
        # recalcular los meses que ya estaban cubiertos.
        larga = IncapacidadEmpleado.objects.create(
            empleado=self.otro_empleado,
            fecha_inicio=date(2026, 3, 1),
            fecha_fin=date(2026, 9, 1),
            tipo=IncapacidadEmpleado.TIPO_RIESGO_TRABAJO,
            estado=IncapacidadEmpleado.ESTADO_ACTIVA,
        )

        self.client.post(
            reverse("rrhh:rrhh_incapacidad_editar", args=[larga.id]),
            {
                "empleado": str(self.otro_empleado.id),
                "fecha_inicio": "2026-03-01",
                "fecha_fin": "2026-09-08",
                "tipo": IncapacidadEmpleado.TIPO_RIESGO_TRABAJO,
                "folio": "",
                "estado": IncapacidadEmpleado.ESTADO_ACTIVA,
                "notas": "",
                "motivo": "Se prorrogó una semana",
            },
        )

        larga.refresh_from_db()
        self.assertEqual(larga.fecha_fin, date(2026, 9, 8))
        self.assertEqual(
            self._dias_evaluados(evaluar),
            {self.otro_empleado.id: {date(2026, 9, d) for d in range(2, 9)}},
        )

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_eliminar_borra_el_registro_y_deja_rastro(self, evaluar):
        # El caso real: un renglón duplicado que quedó cancelado y estorba.
        duplicado = IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 9, 9),
            fecha_fin=date(2026, 9, 10),
            tipo=IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
            estado=IncapacidadEmpleado.ESTADO_CANCELADA,
        )

        self.client.post(
            reverse("rrhh:rrhh_incapacidad_eliminar", args=[duplicado.id]),
            {"motivo_eliminacion": "Renglón duplicado, la buena es la otra"},
        )

        self.assertFalse(IncapacidadEmpleado.objects.filter(pk=duplicado.id).exists())
        rastro = IncapacidadCambio.objects.get(accion=IncapacidadCambio.ACCION_ELIMINAR)
        self.assertIsNone(rastro.incapacidad_id)
        self.assertEqual(rastro.empleado_nombre, str(self.empleado))
        self.assertIn(f"#{duplicado.id}", rastro.resumen)
        self.assertIn("2026-09-09", rastro.resumen)
        self.assertEqual(rastro.realizado_por, self.user)
        self.assertEqual(rastro.motivo, "Renglón duplicado, la buena es la otra")
        # Estaba cancelada: no cubría días, así que no hay nada que reevaluar.
        evaluar.assert_not_called()

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_eliminar_una_activa_libera_sus_dias(self, evaluar):
        self.client.post(
            reverse("rrhh:rrhh_incapacidad_eliminar", args=[self.incapacidad.id]),
            {"motivo_eliminacion": "Capturada por error"},
        )

        self.assertFalse(IncapacidadEmpleado.objects.filter(pk=self.incapacidad.id).exists())
        self.assertEqual(
            self._dias_evaluados(evaluar),
            {self.empleado.id: {date(2026, 7, d) for d in range(1, 6)}},
        )

    @patch("rrhh.views_incapacidades.evaluar_rango_asistencia")
    def test_eliminar_exige_motivo(self, evaluar):
        self.client.post(
            reverse("rrhh:rrhh_incapacidad_eliminar", args=[self.incapacidad.id]),
            {"motivo_eliminacion": "   "},
        )

        self.assertTrue(IncapacidadEmpleado.objects.filter(pk=self.incapacidad.id).exists())
        self.assertFalse(IncapacidadCambio.objects.exists())
        evaluar.assert_not_called()

    def test_eliminar_requiere_permiso(self):
        sin_permiso = get_user_model().objects.create_user(
            username="rrhh-sin-permiso-borrar",
            email="rrhh-sin-permiso-borrar@example.com",
            password="testpass",
        )
        self.client.force_login(sin_permiso)

        r = self.client.post(
            reverse("rrhh:rrhh_incapacidad_eliminar", args=[self.incapacidad.id]),
            {"motivo_eliminacion": "no deberia poder"},
        )

        self.assertEqual(r.status_code, 403)
        self.assertTrue(IncapacidadEmpleado.objects.filter(pk=self.incapacidad.id).exists())

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
