from datetime import date

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import AuditLog
from maestros.models import Proveedor
from reportes.models import AreaPresupuesto, AreaPresupuestoResponsable
from .models import CotizacionCompraDepartamental, ItemCompraDepartamental, SolicitudCompraDepartamental


class GestionSolicitudDepartamentalTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        users = get_user_model().objects
        cls.autora = users.create_user("solicitante")
        cls.gestora = users.create_user("gestora")
        cls.superadmin = users.create_user("superadmin", is_superuser=True)
        cls.area = AreaPresupuesto.objects.create(codigo="ventas", nombre="Ventas")
        administracion = AreaPresupuesto.objects.create(codigo="administracion", nombre="Administración")
        AreaPresupuestoResponsable.objects.create(area=cls.area, usuario=cls.autora, puede_capturar=True)
        AreaPresupuestoResponsable.objects.create(area=administracion, usuario=cls.gestora, puede_capturar=True)
        cls.solicitud = SolicitudCompraDepartamental.objects.create(
            solicitante=cls.autora, area=cls.area, periodo=date(2026, 11, 1),
            estado="ENVIADA", enviada_en=timezone.now(),
        )
        cls.item = ItemCompraDepartamental.objects.create(solicitud=cls.solicitud, descripcion="Estante", cantidad=1)

    def url(self, action):
        return reverse(f"compras:departamental_{action}", args=[self.solicitud.pk])

    def datos(self):
        return {
            "area": self.area.pk, "tipo": "MENSUAL", "periodo": "2026-11",
            "motivo": "Cambio de cantidad", "version": self.solicitud.actualizado_en.isoformat(),
            "items-TOTAL_FORMS": "1", "items-INITIAL_FORMS": "1",
            "items-0-id": self.item.pk, "items-0-descripcion": "Estante",
            "items-0-cantidad": "2", "items-0-unidad": "pieza", "items-0-prioridad": "NORMAL",
        }

    def test_autora_edita_solicitud_enviada_y_conserva_folio_y_articulo(self):
        self.client.force_login(self.autora)
        self.assertContains(self.client.get(self.url("detalle")), self.url("editar"))
        response = self.client.post(self.url("editar"), self.datos())
        self.assertRedirects(response, self.url("detalle"), fetch_redirect_response=False)
        self.item.refresh_from_db()
        self.assertEqual(self.item.cantidad, 2)
        guardada = SolicitudCompraDepartamental.objects.get(pk=self.solicitud.pk)
        self.assertEqual(guardada.folio, self.solicitud.folio)
        self.assertEqual(guardada.enviada_en, self.solicitud.enviada_en)
        self.assertEqual(guardada.estado, "ENVIADA")

    def test_gestora_ajena_no_puede_modificar_y_superadmin_si(self):
        self.client.force_login(self.gestora)
        self.assertNotContains(self.client.get(self.url("detalle")), self.url("editar"))
        self.assertEqual(self.client.post(self.url("editar"), self.datos()).status_code, 403)
        self.assertEqual(self.client.post(self.url("eliminar"), self.datos()).status_code, 403)
        self.client.force_login(self.superadmin)
        listado = reverse("compras:departamental_mis_solicitudes")
        self.assertContains(self.client.get(listado), self.url("detalle"))
        self.assertContains(self.client.get(self.url("detalle")), self.url("editar"))
        self.assertEqual(self.client.post(self.url("editar"), self.datos()).status_code, 302)

    def test_retiro_enviada_conserva_folio_y_bloquea_asignacion(self):
        self.client.force_login(self.autora)
        response = self.client.post(self.url("eliminar"), {
            "motivo": "Ya no se necesita", "version": self.solicitud.actualizado_en.isoformat(),
        })
        self.assertRedirects(response, self.url("detalle"), fetch_redirect_response=False)
        self.solicitud.refresh_from_db()
        self.item.refresh_from_db()
        self.assertEqual(self.solicitud.estado, "CANCELADA")
        self.assertEqual(self.item.estado, "CANCELADO")
        self.client.force_login(self.gestora)
        self.assertEqual(self.client.post(self.url("asignar"), HTTP_ACCEPT="application/json").status_code, 409)

    def test_ni_superadmin_puede_modificar_solicitud_asignada(self):
        SolicitudCompraDepartamental.objects.filter(pk=self.solicitud.pk).update(comprador_asignado=self.gestora)
        self.client.force_login(self.superadmin)
        self.assertEqual(self.client.post(self.url("editar"), self.datos()).status_code, 409)
        self.assertEqual(self.client.post(self.url("eliminar"), self.datos()).status_code, 409)

    def test_solicitante_representada_no_es_autora(self):
        AuditLog.objects.create(
            user=self.gestora, action="COMPRA_SOLICITUD_CREADA",
            model="compras.SolicitudCompraDepartamental", object_id=str(self.solicitud.pk),
        )
        self.client.force_login(self.autora)
        self.assertEqual(self.client.get(self.url("editar")).status_code, 403)
        self.client.force_login(self.gestora)
        self.assertContains(self.client.get(self.url("editar")), "Estante")

    def test_cotizacion_existente_bloquea_aunque_el_estado_siga_enviada(self):
        CotizacionCompraDepartamental.objects.create(
            item=self.item, proveedor=Proveedor.objects.create(nombre="Proveedor de prueba"),
            cantidad_ofertada=1, costo_unitario=10,
        )
        self.client.force_login(self.autora)
        self.assertEqual(self.client.post(self.url("editar"), self.datos()).status_code, 409)
        self.assertEqual(self.client.post(self.url("eliminar"), self.datos()).status_code, 409)

    def test_borrador_eliminado_sale_del_listado_y_el_folio_no_se_reutiliza(self):
        SolicitudCompraDepartamental.objects.filter(pk=self.solicitud.pk).update(estado="BORRADOR", enviada_en=None)
        self.client.force_login(self.autora)
        listado = reverse("compras:departamental_mis_solicitudes")
        self.assertContains(self.client.get(listado), self.solicitud.folio)
        response = self.client.post(self.url("eliminar"), {
            "motivo": "Duplicado", "version": self.solicitud.actualizado_en.isoformat(),
        })
        self.assertRedirects(response, listado, fetch_redirect_response=False)
        self.assertNotContains(self.client.get(listado), self.url("detalle"))
        nueva = SolicitudCompraDepartamental.objects.create(
            solicitante=self.autora, area=self.area, periodo=self.solicitud.periodo,
        )
        self.assertNotEqual(self.solicitud.folio, nueva.folio)
        self.assertTrue(self.solicitud.eventos.filter(tipo="SOLICITUD_BORRADOR_ELIMINADA").exists())

    def test_version_obsoleta_y_articulo_ajeno_no_sobrescriben(self):
        self.client.force_login(self.autora)
        self.assertEqual(self.client.post(self.url("editar"), {**self.datos(), "version": "obsoleta"}).status_code, 409)
        ajena = SolicitudCompraDepartamental.objects.create(
            solicitante=self.gestora, area=self.area, periodo=self.solicitud.periodo,
        )
        otro_item = ItemCompraDepartamental.objects.create(solicitud=ajena, descripcion="Ajeno", cantidad=7)
        self.assertEqual(self.client.post(self.url("editar"), {**self.datos(), "items-0-id": otro_item.pk}).status_code, 400)
        self.item.refresh_from_db()
        otro_item.refresh_from_db()
        self.assertEqual(self.item.cantidad, 1)
        self.assertEqual(otro_item.cantidad, 7)

    def test_articulos_se_pueden_agregar_y_quitar_con_al_menos_un_renglon(self):
        self.client.force_login(self.autora)
        datos = {**self.datos(), "items-0-DELETE": "on"}
        self.assertEqual(self.client.post(self.url("editar"), datos).status_code, 400)
        datos.update({
            "items-TOTAL_FORMS": "2", "items-1-descripcion": "Bandeja",
            "items-1-cantidad": "3", "items-1-unidad": "pieza", "items-1-prioridad": "ALTA",
        })
        self.assertEqual(self.client.post(self.url("editar"), datos).status_code, 302)
        self.assertEqual(list(self.solicitud.items.values_list("descripcion", flat=True)), ["Bandeja"])
