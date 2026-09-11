"""La inspección diaria no debe abrir un ticket nuevo por el mismo desperfecto."""

from datetime import timedelta
from unittest import mock

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.db.models import F
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.access import ACCESS_MANAGE
from core.duplicados import DuplicadoInvalido
from core.models import Sucursal, UserModuleAccess
from logistica.models import InspeccionDiaria, Repartidor, ReporteUnidad, Unidad
from logistica.services_reportes_repetidos import (
    marcar_duplicado_unidad,
    normalizar,
    son_equivalentes,
)

User = get_user_model()

CHECKS_OK = {
    "aceite_ok": True, "refrigerante_ok": True, "liquido_frenos_ok": True,
    "limpiaparabrisas_ok": True, "presion_llantas_ok": True, "desgaste_llantas_ok": True,
    "luces_ok": True, "escobillas_ok": True, "bateria_ok": True, "tablero_ok": True,
    "documentos_ok": True, "licencia_ok": True, "kit_emergencia_ok": True,
}


class EquivalenciaObservacionesTests(TestCase):
    """Casos tomados de los reportes reales acumulados en producción."""

    def test_el_mismo_texto_diario_es_equivalente(self):
        self.assertTrue(son_equivalentes("Check encendido", "Falla detectada en inspección diaria: Check encendido"))
        self.assertTrue(son_equivalentes("check ENCENDIDO ", "Check encendido"))

    def test_variantes_cercanas_se_reconocen(self):
        self.assertTrue(son_equivalentes("Check engine encendido", "Check encendido"))

    def test_problemas_distintos_no_se_funden(self):
        self.assertFalse(son_equivalentes("Luz baja no enciende", "Ocupa cambio de llantas"))
        self.assertFalse(son_equivalentes("Check encendido", "Las luces delanteras fallan"))

    def test_normalizacion_quita_acentos_prefijo_y_puntuacion(self):
        self.assertEqual(
            normalizar("Falla detectada en inspección diaria: ¡Presión, de llantas!"),
            "presion de llantas",
        )

    def test_observacion_vacia_no_hace_match(self):
        self.assertFalse(son_equivalentes("", "Check encendido"))


@mock.patch("logistica.signals.notificar_reporte_nuevo.delay", mock.Mock())
class InspeccionDiariaSinRepetidosTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="repartidor.repet", password="pass12345")
        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.sucursal = Sucursal.objects.create(codigo="REPQA", nombre="Sucursal repetidos", activa=True)
        self.unidad = Unidad.objects.create(codigo="REP-01", descripcion="Unidad repetidos", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(
            user=self.user, sucursal=self.sucursal, unidad_asignada=self.unidad
        )
        self.client.force_login(self.user)

    def _inspeccionar(self, observaciones, *, reporte_existente_id=None):
        # La unidad solo admite una inspección por día; se corren las anteriores
        # un día hacia atrás para simular mañanas consecutivas.
        InspeccionDiaria.objects.all().update(fecha=F("fecha") - timedelta(days=1))
        payload = {**CHECKS_OK, "unidad": self.unidad.id, "observaciones": observaciones, "tablero_ok": False}
        if reporte_existente_id:
            payload["reporte_existente_id"] = reporte_existente_id
        return self.client.post("/api/logistica/inspeccion-diaria/", payload)

    def test_la_misma_falla_cada_dia_no_abre_un_ticket_nuevo(self):
        primera = self._inspeccionar("Check encendido")
        self.assertEqual(primera.status_code, 201)
        self.assertFalse(primera.json()["reporte_reafirmado"])
        reporte_id = primera.json()["reporte_generado_id"]

        for dia in range(1, 4):
            repetida = self._inspeccionar("Check encendido")
            self.assertEqual(repetida.status_code, 201)
            self.assertTrue(repetida.json()["reporte_reafirmado"])
            self.assertEqual(repetida.json()["reporte_generado_id"], reporte_id)

        self.assertEqual(ReporteUnidad.objects.filter(unidad=self.unidad).count(), 1)
        principal = ReporteUnidad.objects.get(pk=reporte_id)
        self.assertEqual(principal.reafirmaciones.count(), 3)
        self.assertIn("Sigue presente", principal.reafirmaciones.first().comentario)

    def test_una_falla_distinta_si_abre_su_propio_ticket(self):
        self._inspeccionar("Check encendido")
        otra = self._inspeccionar("Las luces delanteras fallan")

        self.assertEqual(otra.status_code, 201)
        self.assertFalse(otra.json()["reporte_reafirmado"])
        self.assertEqual(ReporteUnidad.objects.filter(unidad=self.unidad).count(), 2)

    def test_el_repartidor_puede_señalar_el_reporte_aunque_lo_escriba_distinto(self):
        primera = self._inspeccionar("Ocupa cambio de llantas")
        reporte_id = primera.json()["reporte_generado_id"]

        # "Llqntas" no se parece lo suficiente para el candado automático;
        # el repartidor lo señala a mano desde la PWA.
        segunda = self._inspeccionar("Llqntas", reporte_existente_id=reporte_id)

        self.assertTrue(segunda.json()["reporte_reafirmado"])
        self.assertEqual(ReporteUnidad.objects.filter(unidad=self.unidad).count(), 1)

    def test_un_ticket_cerrado_no_absorbe_la_falla_que_reaparece(self):
        primera = self._inspeccionar("Check encendido")
        reporte = ReporteUnidad.objects.get(pk=primera.json()["reporte_generado_id"])
        reporte.estatus = ReporteUnidad.ESTATUS_CERRADO
        reporte.save(update_fields=["estatus"])

        vuelve = self._inspeccionar("Check encendido")

        self.assertFalse(vuelve.json()["reporte_reafirmado"])
        self.assertEqual(ReporteUnidad.objects.filter(unidad=self.unidad).count(), 2)


@mock.patch("logistica.signals.notificar_reporte_nuevo.delay", mock.Mock())
class DuplicadosUnidadEnBandejaTests(TestCase):
    def setUp(self):
        self.gestor = User.objects.create_user(username="mant_unidad", password="pass12345")
        UserModuleAccess.objects.create(user=self.gestor, module="mantenimiento", access=ACCESS_MANAGE)
        UserModuleAccess.objects.create(user=self.gestor, module="logistica", access=ACCESS_MANAGE)
        self.sucursal = Sucursal.objects.create(codigo="BANQA", nombre="Sucursal bandeja", activa=True)
        self.unidad = Unidad.objects.create(codigo="BAN-01", descripcion="Unidad bandeja", sucursal=self.sucursal)
        self.client.force_login(self.gestor)

    def _reporte(self, descripcion):
        return ReporteUnidad.objects.create(
            unidad=self.unidad, tipo=ReporteUnidad.TIPO_OTRO, descripcion=descripcion
        )

    def test_ligar_saca_de_la_bandeja_y_deja_el_conteo(self):
        principal = self._reporte("Llantas delanteras desgastadas")
        repetidos = [self._reporte(f"Repetido {i}") for i in range(3)]
        for repetido in repetidos:
            self.client.post(
                reverse("mantenimiento:mant-duplicado", args=["unidad", repetido.pk]),
                {"principal_id": principal.pk},
            )

        response = self.client.get(reverse("mantenimiento:dashboard"))
        unidades = [item for item in response.context["items"] if item["tipo"] == "unidad"]

        self.assertEqual([item["id"] for item in unidades], [principal.id])
        self.assertEqual(unidades[0]["duplicados_total"], 3)
        for repetido in repetidos:
            repetido.refresh_from_db()
            self.assertEqual(repetido.duplicado_de_id, principal.id)

    def test_cerrar_el_principal_arrastra_a_los_repetidos(self):
        principal = self._reporte("Llantas delanteras desgastadas")
        repetido = self._reporte("Llantas lisas")
        marcar_duplicado_unidad(repetido, principal, self.gestor)

        response = self.client.post(
            f"/api/mantenimiento/bandeja/unidad/{principal.pk}/actualizar/",
            {"estatus": ReporteUnidad.ESTATUS_CERRADO, "comentario": "Se cambiaron las llantas."},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        repetido.refresh_from_db()
        self.assertEqual(repetido.estatus, ReporteUnidad.ESTATUS_CERRADO)

    def test_no_se_liga_contra_si_mismo(self):
        reporte = self._reporte("Unico")
        with self.assertRaises(DuplicadoInvalido):
            marcar_duplicado_unidad(reporte, reporte, self.gestor)
