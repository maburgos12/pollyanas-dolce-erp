from __future__ import annotations

import io
import json

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.management import call_command
from django.test import TestCase

from core.models import Departamento, Sucursal, UserModuleAccess, UserProfile
from rrhh.models import Empleado
from rrhh.services_personnel_audit import build_personnel_identity_audit, normalize_catalog_key


class PersonnelIdentityAuditTests(TestCase):
    def test_normalize_catalog_key_removes_case_accents_and_punctuation(self):
        self.assertEqual(normalize_catalog_key("Guamúchil Centro"), "GUAMUCHIL_CENTRO")
        self.assertEqual(normalize_catalog_key("PRODUCCIÓN"), "PRODUCCION")

    def test_audit_detects_identity_and_catalog_risks_without_writes(self):
        User = get_user_model()
        Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        Departamento.objects.create(codigo="VENTAS", nombre="Ventas")
        Group.objects.get_or_create(name="VENTAS")
        Group.objects.get_or_create(name="ventas")

        orphan_user = User.objects.create_user(username="orphan")
        linked_user = User.objects.create_user(username="linked")
        UserProfile.objects.create(user=linked_user, sucursal=Sucursal.objects.get(codigo="PAYAN"))
        UserModuleAccess.objects.create(user=orphan_user, module="mermas.captura", access="manage")

        jefe = Empleado.objects.create(
            nombre="Jefa sin usuario",
            departamento=Empleado.DEP_VENTAS,
            puesto_operativo="JEFATURA",
        )
        Empleado.objects.create(
            nombre="Empleado sin usuario",
            departamento=Empleado.DEP_VENTAS,
            sucursal="Debug",
            jefe_directo=jefe,
        )
        Empleado.objects.create(
            nombre="Empleado vinculado",
            departamento=Empleado.DEP_VENTAS,
            sucursal="Payán",
            usuario_erp=linked_user,
            jefe_directo=jefe,
        )

        report = build_personnel_identity_audit(limit=10)
        categories = {item["category"]: item for item in report["findings"]}

        self.assertIn("group_case_alias", categories)
        self.assertIn("group_case_duplicate", categories)
        self.assertIn("active_user_without_employee", categories)
        self.assertIn("employee_branch_text_unmapped", categories)
        self.assertIn("authorizer_without_user", categories)
        self.assertTrue(report["dry_run"])
        self.assertEqual(Empleado.objects.count(), 3)
        self.assertEqual(User.objects.count(), 2)

    def test_command_outputs_json_report(self):
        out = io.StringIO()
        call_command("audit_personnel_identity", "--json", stdout=out)

        payload = json.loads(out.getvalue())
        self.assertTrue(payload["dry_run"])
        self.assertIn("summary", payload)
        self.assertIn("findings", payload)
