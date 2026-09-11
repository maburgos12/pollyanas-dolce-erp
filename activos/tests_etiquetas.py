"""Impresión de etiquetas QR: permisos, alcance, formato y datos impresos."""

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import RequestFactory, TestCase
from django.urls import reverse

from core.access import ROLE_ADMIN, ROLE_VENTAS
from core.models import AuditLog, Sucursal

from .models import Activo, OrdenMantenimiento
from .services_pasaporte import svg_qr_activo

User = get_user_model()


class EtiquetasActivosTests(TestCase):
    def setUp(self):
        self.payan = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.leyva = Sucursal.objects.create(codigo="LEYVA", nombre="Leyva")
        self.admin = User.objects.create_user("admin_etiquetas", "admin_et@example.com", "test12345")
        Group.objects.get_or_create(name=ROLE_ADMIN)[0].user_set.add(self.admin)
        self.ventas = User.objects.create_user("ventas_etiquetas", "ventas_et@example.com", "test12345")
        Group.objects.get_or_create(name=ROLE_VENTAS)[0].user_set.add(self.ventas)

        self.activos = [
            Activo.objects.create(
                nombre=f"Equipo {indice:02d}",
                sucursal=self.payan,
                ubicacion="Mostrador",
                marca="ACME",
                modelo=f"HX-{indice}",
                estado=Activo.ESTADO_FUERA_SERVICIO,
                criticidad=Activo.CRITICIDAD_ALTA,
                garantia_hasta="2027-01-01",
                costo_adquisicion=Decimal("98765.43"),
            )
            for indice in range(12)
        ]
        self.client.force_login(self.admin)

    def _imprimir(self, modo, activos):
        return self.client.post(
            reverse("activos:etiquetas_imprimir"),
            {"modo": modo, "activo_id": [str(a.pk) for a in activos]},
        )

    def test_el_selector_exige_gestion_de_activos(self):
        self.client.force_login(self.ventas)
        response = self.client.get(reverse("activos:etiquetas"))
        self.assertEqual(response.status_code, 403)

    def test_imprimir_exige_gestion_de_activos(self):
        self.client.force_login(self.ventas)
        response = self._imprimir("carta", self.activos[:1])
        self.assertEqual(response.status_code, 403)

    def test_el_selector_abre_para_quien_gestiona_activos(self):
        response = self.client.get(reverse("activos:etiquetas"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Equipo 00")

    def test_no_se_imprime_por_GET(self):
        response = self.client.get(reverse("activos:etiquetas_imprimir"))
        self.assertEqual(response.status_code, 405)

    def test_carta_acomoda_diez_etiquetas_por_hoja(self):
        response = self._imprimir("carta", self.activos)
        cuerpo = response.content.decode()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(cuerpo.count('class="sheet"'), 2)
        self.assertEqual(cuerpo.count('class="label"'), 12)

    def test_termica_emite_una_pagina_por_activo(self):
        response = self._imprimir("termica", self.activos[:3])
        cuerpo = response.content.decode()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(cuerpo.count('class="label-page"'), 3)
        self.assertNotIn('class="sheet"', cuerpo)

    def test_cada_etiqueta_lleva_el_qr_de_su_propio_activo(self):
        """El UUID no aparece como texto: va codificado en el trazo del SVG.

        Lo que hay que probar es que la etiqueta de un equipo no lleve el QR de
        otro, así que se compara contra el SVG que el servicio genera para ese
        activo. Que la URL codificada sea la correcta lo cubre
        `activos.tests_pasaporte`.
        """
        peticion = RequestFactory().get("/")
        esperados = [svg_qr_activo(peticion, activo) for activo in self.activos[:2]]

        cuerpo = self._imprimir("termica", self.activos[:2]).content.decode()

        self.assertEqual(cuerpo.count("<svg"), 2)
        self.assertNotEqual(esperados[0], esperados[1])
        for activo, svg in zip(self.activos[:2], esperados):
            self.assertIn(svg, cuerpo)
            self.assertNotIn(str(activo.qr_token), cuerpo)

    def test_no_se_imprime_nada_que_pueda_cambiar_ni_costos(self):
        response = self._imprimir("carta", self.activos[:1])
        cuerpo = response.content.decode()

        self.assertNotIn("Fuera de servicio", cuerpo)
        self.assertNotIn("Criticidad", cuerpo)
        self.assertNotIn("2027-01-01", cuerpo)
        self.assertNotIn("98765.43", cuerpo)

    def test_la_etiqueta_imprime_identidad_y_la_instruccion(self):
        activo = self.activos[0]
        response = self._imprimir("carta", [activo])
        cuerpo = response.content.decode()

        self.assertIn(activo.codigo, cuerpo)
        self.assertIn("Equipo 00", cuerpo)
        self.assertIn("Payán", cuerpo)
        self.assertIn("Mostrador", cuerpo)
        self.assertIn("ACME", cuerpo)
        self.assertIn("Escanea", cuerpo)

    def test_un_activo_sin_marca_no_deja_una_etiqueta_rota(self):
        pelado = Activo.objects.create(nombre="Equipo sin ficha", sucursal=self.payan)
        response = self._imprimir("termica", [pelado])

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Equipo sin ficha")

    def test_un_modo_desconocido_se_rechaza(self):
        response = self._imprimir("cinta", self.activos[:1])
        self.assertEqual(response.status_code, 400)

    def test_sin_seleccion_no_se_genera_una_hoja_vacia(self):
        response = self.client.post(reverse("activos:etiquetas_imprimir"), {"modo": "carta"})
        self.assertEqual(response.status_code, 400)

    def test_los_ids_se_validan_contra_el_queryset_autorizado(self):
        """Un id que no existe no puede colarse en la hoja."""
        response = self.client.post(
            reverse("activos:etiquetas_imprimir"),
            {"modo": "carta", "activo_id": [str(self.activos[0].pk), "999999"]},
        )
        cuerpo = response.content.decode()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(cuerpo.count('class="label"'), 1)

    def test_la_impresion_queda_registrada_como_export(self):
        self._imprimir("termica", self.activos[:2])

        registro = AuditLog.objects.filter(action="EXPORT", model="activos.EtiquetaQR").latest("timestamp")
        self.assertEqual(registro.payload["modo"], "termica")
        self.assertEqual(
            sorted(registro.payload["codigos"]), sorted(a.codigo for a in self.activos[:2])
        )

    def test_el_filtro_por_sucursal_acota_el_selector(self):
        Activo.objects.create(nombre="Equipo de Leyva", sucursal=self.leyva)

        response = self.client.get(reverse("activos:etiquetas"), {"sucursal": self.leyva.pk})

        self.assertContains(response, "Equipo de Leyva")
        self.assertNotContains(response, "Equipo 00")

    def test_la_busqueda_acota_el_selector(self):
        response = self.client.get(reverse("activos:etiquetas"), {"q": "Equipo 05"})

        self.assertContains(response, "Equipo 05")
        self.assertNotContains(response, "Equipo 06")
