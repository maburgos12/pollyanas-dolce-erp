import csv
import importlib
import io
import uuid
from datetime import date
from unittest.mock import patch
from zipfile import ZipFile

from django.contrib.auth.models import User
from django.test import Client, TestCase
from django.urls import reverse

from core.models import AuditLog, UserModuleAccess
from rrhh.models import Empleado, EmpleadoBaja


class GafetesTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.admin = User.objects.create_superuser("admin-gafetes", password="test-password")
        cls.user = User.objects.create_user("sin-acceso-gafetes")
        cls.empleado = Empleado.objects.create(
            codigo="00123", nombre="María Fernanda López García", fecha_ingreso=date(2023, 3, 15),
            rfc="RFC-PRIVADO", curp="CURP-PRIVADA", nss="NSS-PRIVADO", salario_diario=900,
        )

    def public_url(self, token=None):
        return reverse("verificar_gafete", args=[token or self.empleado.gafete_token])

    def action(self, empleado, action):
        return self.client.post(reverse("rrhh:gafete_accion", args=[empleado.pk]), {
            "accion": action, "token_actual": str(empleado.gafete_token or ""),
        })

    def test_alta_manual_y_lote_generan_tokens_unicos(self):
        rows = [Empleado(codigo="QR-BULK-1", nombre="Alta lote uno"), Empleado(codigo="QR-BULK-2", nombre="Alta lote dos")]
        Empleado.objects.bulk_create(rows)
        self.assertEqual(3, len({self.empleado.gafete_token, *(e.gafete_token for e in rows)}))
        self.assertTrue(all(isinstance(e.gafete_token, uuid.UUID) for e in rows))

    def test_publica_solo_datos_acordados_y_sin_cache(self):
        response = self.client.get(self.public_url())
        self.assertContains(response, self.empleado.nombre)
        self.assertContains(response, "00123")
        self.assertContains(response, "15 de marzo de 2023")
        self.assertContains(response, "Empleado activo")
        for private in [self.empleado.rfc, self.empleado.curp, self.empleado.nss, "salario_diario", "/dashboard/"]:
            self.assertNotContains(response, private)
        self.assertIn("no-store", response["Cache-Control"])
        self.assertIn("noindex", response["X-Robots-Tag"])
        self.assertEqual("no-referrer", response["Referrer-Policy"])

    def test_tokens_invalidos_o_inactivos_no_revelan_persona(self):
        for token in [uuid.uuid4(), self.empleado.gafete_token]:
            Empleado.objects.filter(pk=self.empleado.pk).update(activo=False)
            response = self.client.get(self.public_url(token))
            self.assertContains(response, "Gafete sin vigencia", status_code=404)
            self.assertNotContains(response, self.empleado.nombre, status_code=404)

    def test_baja_y_reingreso_no_reutilizan_qr(self):
        old = self.empleado.gafete_token
        EmpleadoBaja.objects.create(empleado=self.empleado, fecha_ingreso=self.empleado.fecha_ingreso, fecha_baja=date(2026, 9, 1))
        self.empleado.refresh_from_db()
        self.assertFalse(self.empleado.activo)
        self.assertIsNone(self.empleado.gafete_token)
        self.assertEqual(404, self.client.get(self.public_url(old)).status_code)
        self.empleado.activo = True
        self.empleado.fecha_ingreso = date(2026, 10, 9)
        self.empleado.save(update_fields=["activo", "fecha_ingreso"])
        self.assertNotEqual(old, self.empleado.gafete_token)
        self.assertEqual(404, self.client.get(self.public_url(old)).status_code)
        self.assertEqual(200, self.client.get(self.public_url(self.empleado.gafete_token)).status_code)

    def test_revocar_emitir_y_ficha_anterior_no_restauran_token(self):
        self.client.force_login(self.admin)
        stale = Empleado.objects.get(pk=self.empleado.pk)
        old = stale.gafete_token
        self.assertEqual(302, self.action(self.empleado, "revocar").status_code)
        stale.nombre = "Nombre corregido"
        stale.save()
        stale.refresh_from_db()
        self.assertIsNone(stale.gafete_token)
        self.assertEqual(404, self.client.get(self.public_url(old)).status_code)
        self.assertEqual(302, self.action(stale, "emitir").status_code)
        stale.refresh_from_db()
        self.assertIsNotNone(stale.gafete_token)
        self.assertNotEqual(old, stale.gafete_token)
        self.assertEqual(404, self.client.get(self.public_url(old)).status_code)
        self.assertEqual(2, AuditLog.objects.filter(model="rrhh.Empleado", object_id=str(stale.pk)).count())
        self.assertNotIn(str(old), str(list(AuditLog.objects.values_list("payload", flat=True))))

    def test_accion_desde_pagina_vieja_no_revoca_qr_nuevo(self):
        self.client.force_login(self.admin)
        old = self.empleado.gafete_token
        self.action(self.empleado, "revocar")
        self.empleado.refresh_from_db()
        self.action(self.empleado, "emitir")
        self.empleado.refresh_from_db()
        current = self.empleado.gafete_token
        self.client.post(reverse("rrhh:gafete_accion", args=[self.empleado.pk]), {"accion": "revocar", "token_actual": str(old)})
        self.empleado.refresh_from_db()
        self.assertEqual(current, self.empleado.gafete_token)

    def test_no_emite_inactivo_ni_rota_qr_al_reimprimir(self):
        self.client.force_login(self.admin)
        current = self.empleado.gafete_token
        self.action(self.empleado, "emitir")
        self.client.get(reverse("rrhh:gafete_qr", args=[self.empleado.pk]))
        self.empleado.refresh_from_db()
        self.assertEqual(current, self.empleado.gafete_token)
        self.empleado.activo = False
        self.empleado.save(update_fields=["activo"])
        self.action(self.empleado, "emitir")
        self.empleado.refresh_from_db()
        self.assertIsNone(self.empleado.gafete_token)

    def test_permisos_y_metodos(self):
        urls = [reverse("rrhh:gafetes"), reverse("rrhh:gafetes_descargar"), reverse("rrhh:gafete_qr", args=[self.empleado.pk])]
        for url in urls:
            self.assertEqual(302, self.client.get(url).status_code)
        self.client.force_login(self.user)
        for url in urls:
            self.assertEqual(403, self.client.get(url).status_code)
        self.assertEqual(403, self.action(self.empleado, "revocar").status_code)
        UserModuleAccess.objects.create(user=self.user, module="rrhh.empleados", access="view")
        self.assertEqual(200, self.client.get(urls[0]).status_code)
        self.assertEqual(403, self.client.get(urls[1]).status_code)
        self.client.force_login(self.admin)
        self.assertEqual(405, self.client.get(reverse("rrhh:gafete_accion", args=[self.empleado.pk])).status_code)
        self.assertEqual(405, self.client.post(self.public_url()).status_code)

    def test_revocacion_exige_csrf(self):
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.admin)
        response = client.post(reverse("rrhh:gafete_accion", args=[self.empleado.pk]), {"accion": "revocar", "token_actual": str(self.empleado.gafete_token)}, HTTP_ACCEPT="application/json")
        self.assertRedirects(response, reverse("login"), fetch_redirect_response=False)
        original = self.empleado.gafete_token
        self.empleado.refresh_from_db()
        self.assertEqual(original, self.empleado.gafete_token)

    def test_exportacion_completa_con_svg_y_manifest_seguro(self):
        self.client.force_login(self.admin)
        Empleado.objects.create(codigo="=1+1", nombre="=HYPERLINK(1)")
        inactive = Empleado.objects.create(codigo="NO-EXPORT", nombre="Inactivo", activo=False)
        with patch("rrhh.views_gafetes.segno.make_qr", wraps=__import__("segno").make_qr) as make_qr:
            response = self.client.get(reverse("rrhh:gafetes_descargar"))
        self.assertEqual(200, response.status_code)
        self.assertEqual(2, make_qr.call_count)
        self.assertIn(self.public_url(), make_qr.call_args_list[1].args[0])
        with ZipFile(io.BytesIO(response.content)) as archive:
            rows = list(csv.DictReader(io.StringIO(archive.read("colaboradores.csv").decode("utf-8-sig"))))
            self.assertEqual(2, len(rows))
            self.assertTrue(any(row["codigo_colaborador"] == "00123" for row in rows))
            self.assertTrue(any(row["codigo_colaborador"] == "'=1+1" for row in rows))
            self.assertFalse(any(row["codigo_colaborador"] == inactive.codigo for row in rows))
            for row in rows:
                self.assertIn(b"<svg", archive.read(row["archivo_qr"]))
                self.assertIn("/gafetes/", row["liga_verificacion"])

    def test_publica_tambien_con_sesion_operativa_restringida(self):
        self.client.force_login(self.user)
        for predicate in ["is_branch_capture_only", "is_bonos_produccion_capture_only", "is_repartidor_only", "is_mermas_only"]:
            with self.subTest(predicate=predicate), patch("core.middleware." + predicate, return_value=True):
                self.assertEqual(200, self.client.get(self.public_url()).status_code)

    def test_backfill_migracion_no_cambia_codigos_ni_repite_tokens(self):
        Empleado.objects.filter(pk=self.empleado.pk).update(gafete_token=None)
        inactive = Empleado.objects.create(codigo="BAJA-MIG", nombre="Baja", activo=False)
        migration = importlib.import_module("rrhh.migrations.0054_empleado_gafete_token")
        from django.apps import apps
        from types import SimpleNamespace
        editor = SimpleNamespace(connection=SimpleNamespace(alias="default"))
        migration.emitir_existentes(apps, editor)
        self.empleado.refresh_from_db()
        token = self.empleado.gafete_token
        migration.emitir_existentes(apps, editor)
        self.empleado.refresh_from_db()
        inactive.refresh_from_db()
        self.assertEqual(token, self.empleado.gafete_token)
        self.assertIsNone(inactive.gafete_token)
        self.assertEqual("00123", self.empleado.codigo)
