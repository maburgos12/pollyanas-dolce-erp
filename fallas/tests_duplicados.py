"""Reportes repetidos ligados al que ya se está atendiendo."""

from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.access import ACCESS_MANAGE
from core.models import Sucursal, UserModuleAccess
from fallas.models import BitacoraFalla, CategoriaFalla, ReporteFalla
from fallas.services_duplicados import DuplicadoInvalido, marcar_duplicado, principal_de


class DuplicadosFallasTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.gestor = user_model.objects.create_user(username="mant_dup", password="test12345")
        UserModuleAccess.objects.create(user=self.gestor, module="mantenimiento", access=ACCESS_MANAGE)
        UserModuleAccess.objects.create(user=self.gestor, module="fallas", access=ACCESS_MANAGE)
        self.reportero = user_model.objects.create_user(
            username="sucursal_dup", password="test12345", email="sucursal@example.com"
        )
        self.sucursal = Sucursal.objects.create(codigo="DUPQA", nombre="Sucursal QA", activa=True)
        self.categoria = CategoriaFalla.objects.create(nombre="Flota", tipo=CategoriaFalla.TIPO_EQUIPO)

    def _falla(self, titulo):
        return ReporteFalla.objects.create(
            sucursal=self.sucursal,
            categoria=self.categoria,
            titulo=titulo,
            descripcion="Llanta trasera lisa.",
            justificacion_sin_foto="Sin cámara a la mano.",
            reportado_por=self.reportero,
        )

    def test_ligar_repetido_lo_saca_de_pendientes_sin_borrarlo(self):
        principal = self._falla("Llantas de la unidad 3")
        repetido = self._falla("Llantas lisas unidad 3")

        marcar_duplicado(repetido, principal, self.gestor)

        repetido.refresh_from_db()
        self.assertEqual(repetido.duplicado_de_id, principal.id)
        # Sigue existiendo: la recurrencia es el dato que justifica el cambio de fondo.
        self.assertTrue(ReporteFalla.objects.filter(pk=repetido.pk).exists())
        self.assertEqual(principal.duplicados.count(), 1)
        self.assertTrue(BitacoraFalla.objects.filter(reporte=principal, comentario__contains=str(repetido.pk)).exists())
        self.assertTrue(BitacoraFalla.objects.filter(reporte=repetido, comentario__contains=str(principal.pk)).exists())

    def test_cerrar_el_principal_arrastra_a_los_repetidos(self):
        principal = self._falla("Llantas de la unidad 3")
        repetidos = [self._falla(f"Repetido {i}") for i in range(3)]
        for repetido in repetidos:
            marcar_duplicado(repetido, principal, self.gestor)

        self.client.force_login(self.gestor)
        # Sin el mock la prueba se va a reintentar contra Redis; además así se
        # comprueba que a cada quien se le avisa, no solo que cambió el estatus.
        with mock.patch("fallas.tasks.notificar_cambio_estatus.delay") as aviso:
            response = self.client.post(
                f"/api/fallas/reportes/{principal.pk}/estatus/",
                {"estatus": ReporteFalla.ESTATUS_CERRADO, "comentario": "Se cambiaron las 4 llantas."},
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 200)
        for repetido in repetidos:
            repetido.refresh_from_db()
            self.assertEqual(repetido.estatus, ReporteFalla.ESTATUS_CERRADO)
            self.assertIsNotNone(repetido.fecha_cierre)
        notificados = {llamada.args[0] for llamada in aviso.call_args_list}
        self.assertEqual(notificados, {principal.pk} | {r.pk for r in repetidos})

    def test_cadena_de_duplicados_queda_plana(self):
        principal = self._falla("Principal")
        medio = self._falla("Medio")
        ultimo = self._falla("Ultimo")

        marcar_duplicado(medio, principal, self.gestor)
        destino = marcar_duplicado(ultimo, medio, self.gestor)

        ultimo.refresh_from_db()
        self.assertEqual(destino.pk, principal.pk)
        self.assertEqual(ultimo.duplicado_de_id, principal.id)
        self.assertEqual(principal_de(ultimo).pk, principal.pk)

    def test_rechaza_ciclos_y_autoreferencia(self):
        uno = self._falla("Uno")
        dos = self._falla("Dos")
        marcar_duplicado(dos, uno, self.gestor)

        with self.assertRaises(DuplicadoInvalido):
            marcar_duplicado(uno, uno, self.gestor)
        with self.assertRaises(DuplicadoInvalido):
            marcar_duplicado(uno, dos, self.gestor)

    def test_ligar_arrastra_los_repetidos_que_ya_colgaban(self):
        principal = self._falla("Principal")
        medio = self._falla("Medio")
        colgado = self._falla("Colgado del medio")
        marcar_duplicado(colgado, medio, self.gestor)

        marcar_duplicado(medio, principal, self.gestor)

        colgado.refresh_from_db()
        self.assertEqual(colgado.duplicado_de_id, principal.id)
        self.assertEqual(principal.duplicados.count(), 2)

    def test_un_broker_caido_no_tumba_la_vinculacion(self):
        """El aviso corre en on_commit: si revienta ahí, la petición ya no puede fallar."""
        principal = self._falla("Llantas de la unidad 3")
        repetido = self._falla("Llantas lisas unidad 3")
        self.client.force_login(self.gestor)

        with mock.patch(
            "fallas.tasks.notificar_duplicado_vinculado.delay",
            side_effect=OSError("Error 61 connecting to localhost:6379."),
        ):
            response = self.client.post(
                reverse("mantenimiento:mant-duplicado", args=["falla", repetido.pk]),
                {"principal_id": principal.pk},
                follow=True,
            )

        self.assertEqual(response.status_code, 200)
        repetido.refresh_from_db()
        self.assertEqual(repetido.duplicado_de_id, principal.id)

    def test_la_bandeja_solo_muestra_el_principal_con_su_conteo(self):
        principal = self._falla("Llantas de la unidad 3")
        for i in range(3):
            marcar_duplicado(self._falla(f"Repetido {i}"), principal, self.gestor)
        self.client.force_login(self.gestor)

        response = self.client.get(reverse("mantenimiento:dashboard"))

        fallas_en_bandeja = [item for item in response.context["items"] if item["tipo"] == "falla"]
        self.assertEqual([item["id"] for item in fallas_en_bandeja], [principal.id])
        self.assertEqual(fallas_en_bandeja[0]["duplicados_total"], 3)
        self.assertContains(response, "Reportado 4 veces")

    def test_la_vista_liga_desde_la_bandeja_y_valida_la_seleccion(self):
        principal = self._falla("Llantas de la unidad 3")
        repetido = self._falla("Llantas lisas unidad 3")
        self.client.force_login(self.gestor)
        url = reverse("mantenimiento:mant-duplicado", args=["falla", repetido.pk])

        sin_seleccion = self.client.post(url, {"principal_id": ""}, follow=True)
        repetido.refresh_from_db()
        self.assertIsNone(repetido.duplicado_de_id)
        self.assertIn("Selecciona el reporte", " ".join(str(m) for m in sin_seleccion.context["messages"]))

        ok = self.client.post(url, {"principal_id": principal.pk}, follow=True)
        repetido.refresh_from_db()
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(repetido.duplicado_de_id, principal.id)
