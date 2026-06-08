from __future__ import annotations

import io
import json

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.management import call_command
from django.test import TestCase

from core.models import Departamento, Sucursal, UserModuleAccess, UserProfile
from logistica.models import Repartidor
from rrhh.models import Empleado
from rrhh.services_personnel_identity_sync import build_personnel_identity_projection_plan
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
        repartidor_rows = [item for item in report["proposals"] if item["action"] == "vincular_usuario_repartidor"]
        self.assertTrue(repartidor_rows)
        self.assertIn("usuario real que ya usa en app logistica", repartidor_rows[0]["proposed_value"])
        self.assertEqual(UserProfile.objects.count(), 0)
        self.assertEqual(Empleado.objects.count(), 2)

    def test_external_logistics_account_is_not_forced_into_employee_rrhh(self):
        User = get_user_model()
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz")
        group = Group.objects.get_or_create(name="repartidor")[0]
        external_user = User.objects.create_user(
            username="marcortez.alvarez.vea",
            first_name="Bernardo",
            last_name="Alvarez Vea",
        )
        external_user.groups.add(group)
        Repartidor.objects.create(
            user=external_user,
            sucursal=sucursal,
            tipo_identidad=Repartidor.TIPO_EXTERNO_AUTORIZADO,
            empresa_externa="Empresa externa",
            motivo_autorizacion="Uso ocasional de unidades",
            autorizado_por="Dirección",
        )

        report = build_personnel_normalization_plan(limit=100)
        actions = {item["action"] for item in report["proposals"]}

        self.assertIn("cuenta_externa_logistica_autorizada", actions)
        self.assertNotIn("revisar_usuario_repartidor_sin_empleado", actions)
        self.assertNotIn("vincular_usuario_o_crear_perfil", actions)
        external_rows = [
            item for item in report["proposals"] if item["action"] == "cuenta_externa_logistica_autorizada"
        ]
        self.assertEqual(external_rows[0]["severity"], "info")
        self.assertIn("sin crear empleado Dolce", external_rows[0]["reason"])

    def test_authorized_technical_accounts_are_not_forced_into_employee_rrhh(self):
        User = get_user_model()
        User.objects.create_user(username="ad_agent_service")
        User.objects.create_user(username="omnichannel_service")
        User.objects.create_user(username="debug-rrhh")

        report = build_personnel_normalization_plan(limit=100)
        rows_by_user = {}
        for item in report["proposals"]:
            rows_by_user.setdefault(item["display"], []).append(item["action"])

        self.assertEqual(
            rows_by_user["ad_agent_service"],
            ["cuenta_tecnica_autorizada"],
        )
        self.assertEqual(
            rows_by_user["omnichannel_service"],
            ["cuenta_tecnica_autorizada"],
        )
        self.assertIn("clasificar_cuenta_no_personal", rows_by_user["debug-rrhh"])
        self.assertIn("clasificar_usuario_sin_empleado", rows_by_user["debug-rrhh"])

    def test_production_leadership_is_not_forced_into_embetunado_position(self):
        Empleado.objects.create(
            nombre="Encargada Produccion",
            departamento=Empleado.DEP_PRODUCCION,
            area="PRODUCCION",
            puesto_operativo="",
            nivel_organizacional=Empleado.NIVEL_ENCARGADA,
        )

        report = build_personnel_normalization_plan(limit=100)
        actions = {item["action"] for item in report["proposals"]}

        self.assertNotIn("validar_produccion_vs_embetunado", actions)

    def test_occasional_driver_is_not_reported_as_mixed_repartidor_role(self):
        User = get_user_model()
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz")
        production_group = Group.objects.get_or_create(name="PRODUCCION")[0]
        repartidor_group = Group.objects.get_or_create(name="repartidor")[0]
        user = User.objects.create_user(username="carolina.cayetano")
        user.groups.add(production_group, repartidor_group)
        Empleado.objects.create(
            nombre="Carolina Cayetano",
            departamento=Empleado.DEP_PRODUCCION,
            area="PRODUCCION",
            nivel_organizacional=Empleado.NIVEL_JEFATURA,
            usuario_erp=user,
        )
        Repartidor.objects.create(
            user=user,
            sucursal=sucursal,
            tipo_identidad=Repartidor.TIPO_EMPLEADO_CONDUCTOR_OCASIONAL,
            motivo_autorizacion="Uso ocasional de unidades",
            autorizado_por="Direccion",
        )

        report = build_personnel_normalization_plan(limit=100)
        actions = {item["action"] for item in report["proposals"]}

        self.assertIn("conductor_occasional_logistica_autorizado", actions)
        self.assertNotIn("separar_grupos_repartidor", actions)
        self.assertNotIn("revisar_usuario_con_multiples_grupos", actions)

        user.groups.remove(repartidor_group)
        report_without_group = build_personnel_normalization_plan(limit=100)
        actions_without_group = {item["action"] for item in report_without_group["proposals"]}
        self.assertIn("conductor_occasional_logistica_autorizado", actions_without_group)
        self.assertNotIn("separar_grupos_repartidor", actions_without_group)

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


class PersonnelIdentityProjectionTests(TestCase):
    def test_dry_run_reports_safe_projection_without_writes(self):
        User = get_user_model()
        Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        Departamento.objects.create(codigo="VENTAS", nombre="Ventas")
        usuario = User.objects.create_user(username="repartidor.demo", password="pass123")
        Empleado.objects.create(
            nombre="Repartidor Demo",
            departamento=Empleado.DEP_VENTAS,
            puesto_operativo="REPARTIDOR",
            sucursal="Matriz",
            usuario_erp=usuario,
        )

        report = build_personnel_identity_projection_plan(limit=100)
        actions = {item["action"] for item in report["actions"]}

        self.assertTrue(report["dry_run"])
        self.assertFalse(report["writes"])
        self.assertFalse(report["include_repartidores"])
        self.assertIn("sincronizar_nombre_usuario_desde_empleado", actions)
        self.assertIn("crear_userprofile_desde_empleado_vinculado", actions)
        self.assertIn("crear_repartidor_logistica_desde_empleado_vinculado", actions)
        self.assertEqual(UserProfile.objects.count(), 0)
        self.assertFalse(hasattr(usuario, "repartidor_logistica"))
        usuario.refresh_from_db()
        self.assertEqual(usuario.get_full_name(), "")

    def test_apply_preserves_login_groups_and_explicit_access(self):
        User = get_user_model()
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        departamento = Departamento.objects.create(codigo="VENTAS", nombre="Ventas")
        grupo = Group.objects.create(name="VENTAS")
        usuario = User.objects.create_user(username="ventas.demo", password="pass123")
        usuario.groups.add(grupo)
        UserModuleAccess.objects.create(user=usuario, module="ventas", access=UserModuleAccess.ACCESS_MANAGE)
        Empleado.objects.create(
            nombre="Ventas Demo",
            departamento=Empleado.DEP_VENTAS,
            sucursal="Matriz",
            usuario_erp=usuario,
        )

        report = build_personnel_identity_projection_plan(apply=True, limit=100)

        usuario.refresh_from_db()
        profile = UserProfile.objects.get(user=usuario)
        self.assertFalse(report["dry_run"])
        self.assertTrue(report["writes"])
        self.assertEqual(report["summary"]["applied"], 2)
        self.assertTrue(usuario.check_password("pass123"))
        self.assertTrue(usuario.is_active)
        self.assertFalse(usuario.is_staff)
        self.assertFalse(usuario.is_superuser)
        self.assertEqual(list(usuario.groups.values_list("name", flat=True)), ["VENTAS"])
        self.assertTrue(UserModuleAccess.objects.filter(user=usuario, module="ventas", access="manage").exists())
        self.assertEqual(usuario.get_full_name(), "Ventas Demo")
        self.assertEqual(profile.departamento, departamento)
        self.assertEqual(profile.sucursal, sucursal)

    def test_apply_does_not_guess_unlinked_repartidor_user(self):
        User = get_user_model()
        Group.objects.create(name="repartidor")
        usuario = User.objects.create_user(username="rep.existente", password="pass123")
        usuario.groups.add(Group.objects.get(name="repartidor"))
        Empleado.objects.create(
            nombre="Repartidor Sin Liga",
            departamento=Empleado.DEP_LOGISTICA,
            puesto_operativo="REPARTIDOR",
            sucursal="Matriz",
        )

        report = build_personnel_identity_projection_plan(apply=True, include_repartidores=True, limit=100)

        self.assertEqual(report["summary"]["actions"], 0)
        self.assertFalse(Empleado.objects.get(nombre="Repartidor Sin Liga").usuario_erp_id)
        self.assertFalse(hasattr(usuario, "empleado_rrhh"))

    def test_repartidor_projection_requires_explicit_flag(self):
        from logistica.models import Repartidor

        User = get_user_model()
        Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        usuario = User.objects.create_user(username="rep.demo", password="pass123")
        Empleado.objects.create(
            nombre="Repartidor Ligado",
            departamento=Empleado.DEP_LOGISTICA,
            puesto_operativo="REPARTIDOR",
            sucursal="Matriz",
            usuario_erp=usuario,
        )

        build_personnel_identity_projection_plan(apply=True, limit=100)
        self.assertFalse(Repartidor.objects.filter(user=usuario).exists())

        build_personnel_identity_projection_plan(apply=True, include_repartidores=True, limit=100)
        self.assertTrue(Repartidor.objects.filter(user=usuario, sucursal__codigo="MATRIZ").exists())
        self.assertTrue(usuario.groups.filter(name="repartidor").exists())

    def test_projection_command_outputs_json(self):
        out = io.StringIO()
        call_command("sync_personnel_identity_projections", "--json", stdout=out)

        payload = json.loads(out.getvalue())
        self.assertTrue(payload["dry_run"])
        self.assertFalse(payload["writes"])
        self.assertIn("no_cambia_passwords", payload["guardrails"])
