from __future__ import annotations

import io
import json

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.management import call_command
from django.test import TestCase

from core.models import Departamento, Sucursal, UserModuleAccess, UserProfile
from rrhh.models import Empleado
from rrhh.services_personnel_normalization import build_personnel_normalization_plan
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


class PersonnelNormalizationPlanTests(TestCase):
    def test_plan_builds_reviewable_rows_without_writes(self):
        User = get_user_model()
        Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        Departamento.objects.create(codigo="VENTAS", nombre="Ventas")
        Group.objects.get_or_create(name="VENTAS")
        Group.objects.get_or_create(name="ventas")

        linked_user = User.objects.create_user(username="jefa.produccion")
        repartidor_user = User.objects.create_user(username="repartidor.demo")
        repartidor_user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        repartidor_user.groups.add(Group.objects.get_or_create(name="PRODUCCION")[0])

        Empleado.objects.create(
            nombre="Jefa Produccion",
            departamento=Empleado.DEP_PRODUCCION,
            area="PRODUCCION",
            puesto_operativo="JEFATURA",
            sucursal="Debug",
            usuario_erp=linked_user,
        )
        Empleado.objects.create(
            nombre="Repartidor Demo",
            departamento=Empleado.DEP_VENTAS,
            area="VENTAS",
            puesto_operativo="REPARTIDOR",
            sucursal="",
        )

        report = build_personnel_normalization_plan(limit=100)
        actions = {item["action"] for item in report["proposals"]}

        self.assertTrue(report["dry_run"])
        self.assertFalse(report["writes"])
        self.assertIn("revisar_fusion_grupo_mayusculas", actions)
        self.assertIn("separar_jefatura_de_puesto_operativo", actions)
        self.assertIn("resolver_sucursal_legacy_no_mapeada", actions)
        self.assertIn("crear_perfil_desde_empleado_vinculado", actions)
        self.assertIn("vincular_usuario_repartidor", actions)
        self.assertIn("separar_grupos_repartidor", actions)
        self.assertEqual(UserProfile.objects.count(), 0)
        self.assertEqual(Empleado.objects.count(), 2)

    def test_command_outputs_markdown_table_and_json(self):
        out = io.StringIO()
        call_command("plan_personnel_normalization", "--limit", "5", stdout=out)
        text = out.getvalue()

        self.assertIn("# Plan de normalizacion de personal (dry-run)", text)
        self.assertIn("| Sev | Plano | Accion | Entidad | Actual | Propuesto | Auto | Razon |", text)

        json_out = io.StringIO()
        call_command("plan_personnel_normalization", "--json", stdout=json_out)
        payload = json.loads(json_out.getvalue())

        self.assertTrue(payload["dry_run"])
        self.assertFalse(payload["writes"])
        self.assertIn("summary", payload)
        self.assertIn("proposals", payload)
