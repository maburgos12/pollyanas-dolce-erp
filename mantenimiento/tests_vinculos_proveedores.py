from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import close_old_connections
from django.test import TestCase, TransactionTestCase, Client
from django.urls import reverse
from core.models import AuditLog, UserModuleAccess, UserProfile
from maestros.models import Proveedor
from .models import ProveedorServicio, VinculoProveedorDocumental
from .services_vinculos_proveedores import confirmar_vinculo


class VinculosDocumentalesTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_user(username="gestor_documental")
        for module in ("mantenimiento", "maestros.proveedores"):
            UserModuleAccess.objects.create(user=self.actor, module=module, access="manage")
        self.perfil = ProveedorServicio.objects.create(nombre="Técnico", contacto="Conservar contacto")
        self.proveedor = Proveedor.objects.create(nombre="Comercial", lead_time_dias=12)
        self.url = reverse("mantenimiento:vinculos-proveedores")
        self.data = dict(perfil_id=self.perfil.pk, proveedor_id=self.proveedor.pk,
                         motivo="Contrato de servicio", evidencia="Contrato folio 123", confirmado=True)
        self.client.force_login(self.actor)

    def confirmar(self, **kwargs):
        return confirmar_vinculo(user=self.actor, **{**self.data, **kwargs})

    def test_many_pairs_replay_original_author_and_sources_unchanged(self):
        original_perfil = ProveedorServicio.objects.values().get(pk=self.perfil.pk)
        original_proveedor = Proveedor.objects.values().get(pk=self.proveedor.pk)
        vinculo, created = self.confirmar()
        self.assertTrue(created)
        second = get_user_model().objects.create_superuser("otro", password="test")
        replay, created = confirmar_vinculo(user=second, **self.data)
        self.assertFalse(created)
        self.assertEqual(replay.autor_id, self.actor.pk)
        self.assertEqual(replay.creado_en, vinculo.creado_en)
        other_profile = ProveedorServicio.objects.create(nombre="Otro perfil")
        other_supplier = Proveedor.objects.create(nombre="Otro comercial")
        self.confirmar(perfil_id=other_profile.pk)
        self.confirmar(proveedor_id=other_supplier.pk)
        self.assertEqual(VinculoProveedorDocumental.objects.count(), 3)
        self.assertEqual(AuditLog.objects.filter(model="mantenimiento.VinculoProveedorDocumental").count(), 3)
        self.assertEqual(ProveedorServicio.objects.values().get(pk=self.perfil.pk), original_perfil)
        self.assertEqual(Proveedor.objects.values().get(pk=self.proveedor.pk), original_proveedor)
        with self.assertRaises(ValidationError):
            self.confirmar(evidencia="Cambio no permitido")
        vinculo.refresh_from_db()
        self.assertEqual(vinculo.evidencia, self.data["evidencia"])

    def test_missing_or_read_only_either_permission_fresh_revocation(self):
        # Prime original object's explicit cache; service must reload it.
        from core.access import can_manage_submodule
        self.assertTrue(can_manage_submodule(self.actor, "maestros", "proveedores"))
        for module in ("mantenimiento", "maestros.proveedores"):
            row = UserModuleAccess.objects.get(user=self.actor, module=module)
            for access in ("view", "none"):
                row.access = access
                row.save()
                with self.assertRaises(PermissionDenied):
                    self.confirmar()
            row.access = "manage"
            row.save()
        self.assertEqual(VinculoProveedorDocumental.objects.count(), 0)

    def test_missing_capability_and_group_revoke_never_exposes_sources(self):
        for module in ("mantenimiento", "maestros.proveedores"):
            row = UserModuleAccess.objects.get(user=self.actor, module=module)
            row.delete()
            with self.assertRaises(PermissionDenied):
                self.confirmar()
            response = self.client.get(self.url)
            self.assertEqual(response.status_code, 403)
            self.assertNotIn(b"Conservar contacto", response.content)
            UserModuleAccess.objects.create(user=self.actor, module=module, access="manage")
        self.actor.groups.add(Group.objects.get_or_create(name="mantenimiento")[0])
        UserModuleAccess.objects.filter(user=self.actor, module="mantenimiento").update(access="view")
        with self.assertRaises(PermissionDenied):
            self.confirmar()
        self.assertEqual(self.client.get(self.url).status_code, 200)
        UserModuleAccess.objects.filter(user=self.actor, module="mantenimiento").update(access="none")
        with self.assertRaises(PermissionDenied):
            self.confirmar()
        self.assertEqual(self.client.get(self.url).status_code, 403)

    def test_all_child_overrides_limit_legacy_group_and_parent_manage_remains_valid(self):
        self.actor.groups.add(Group.objects.get_or_create(name="mantenimiento")[0])
        UserModuleAccess.objects.filter(user=self.actor, module="mantenimiento").delete()
        for submodule in ("app", "bandeja", "dashboard"):
            UserModuleAccess.objects.create(user=self.actor, module=f"mantenimiento.{submodule}", access="none")
        with self.assertRaises(PermissionDenied):
            self.confirmar()
        self.assertEqual(self.client.get(self.url).status_code, 403)
        UserModuleAccess.objects.filter(user=self.actor, module="mantenimiento.app").update(access="view")
        with self.assertRaises(PermissionDenied):
            self.confirmar()
        self.assertEqual(self.client.get(self.url).status_code, 200)
        UserModuleAccess.objects.create(user=self.actor, module="mantenimiento", access="manage")
        self.assertTrue(self.confirmar()[1])

    def test_inactive_account_and_locked_master(self):
        UserProfile.objects.update_or_create(user=self.actor, defaults={"lock_maestros": True})
        with self.assertRaises(PermissionDenied):
            self.confirmar()
        UserProfile.objects.filter(user=self.actor).update(lock_maestros=False)
        get_user_model().objects.filter(pk=self.actor.pk).update(is_active=False)
        with self.assertRaises(PermissionDenied):
            self.confirmar()

    def test_validation_deleted_inactive_and_atomic_audit_failure(self):
        for values in ({"confirmado": False}, {"motivo": " "}, {"evidencia": ""}, {"perfil_id": "missing"}, {"proveedor_id": 99999}):
            with self.assertRaises(ValidationError):
                self.confirmar(**values)
        for source in (self.perfil, self.proveedor):
            source.activo = False
            source.save()
            with self.assertRaises(ValidationError):
                self.confirmar()
            source.activo = True
            source.save()
        with patch("mantenimiento.services_vinculos_proveedores.AuditLog.objects.create", side_effect=RuntimeError("audit unavailable")):
            with self.assertRaises(RuntimeError):
                self.confirmar()
        self.assertFalse(VinculoProveedorDocumental.objects.exists())
        self.assertFalse(AuditLog.objects.filter(model="mantenimiento.VinculoProveedorDocumental").exists())

    def test_deletion_preserves_provenance_and_does_not_restore_sources(self):
        vinculo, _ = self.confirmar()
        self.perfil.delete()
        self.proveedor.delete()
        vinculo.refresh_from_db()
        self.assertIsNone(vinculo.perfil_id)
        self.assertIsNone(vinculo.proveedor_id)
        self.assertEqual(vinculo.perfil_original_id, self.data["perfil_id"])
        self.assertEqual(vinculo.proveedor_original_id, self.data["proveedor_id"])
        with self.assertRaises(ValidationError):
            self.confirmar()
        self.assertFalse(ProveedorServicio.objects.exists())
        self.assertFalse(Proveedor.objects.exists())
        self.assertTrue(AuditLog.objects.filter(object_id=str(vinculo.pk), model="mantenimiento.VinculoProveedorDocumental").exists())
        original_author = self.actor.pk
        get_user_model().objects.filter(pk=self.actor.pk).delete()
        vinculo.refresh_from_db()
        self.assertIsNone(vinculo.autor_id)
        self.assertEqual(vinculo.autor_original_id, original_author)
        self.assertTrue(AuditLog.objects.filter(object_id=str(vinculo.pk), model="mantenimiento.VinculoProveedorDocumental", user__isnull=True).exists())
        with self.assertRaises(PermissionDenied):
            self.confirmar()

    def test_get_csrf_read_gate_and_async_html_context(self):
        payload = {**self.data, "confirmado": "on"}
        self.assertEqual(self.client.get(self.url, payload).status_code, 200)
        self.assertFalse(VinculoProveedorDocumental.objects.exists())
        csrf_client = Client(enforce_csrf_checks=True)
        csrf_client.force_login(self.actor)
        # Existing shared CSRF failure view redirects to login. Rejection
        # must happen before the write service, including async requests.
        rejected = csrf_client.post(self.url, payload)
        self.assertEqual(rejected.status_code, 302)
        self.assertIn("/login/", rejected["Location"])
        self.assertFalse(VinculoProveedorDocumental.objects.exists())
        self.assertEqual(csrf_client.post(self.url, payload, HTTP_ACCEPT="application/json").status_code, 302)
        self.assertFalse(VinculoProveedorDocumental.objects.exists())
        html = self.client.post(self.url, payload)
        self.assertEqual(html.status_code, 200)
        self.assertContains(html, "Contrato folio 123")
        response = self.client.post(self.url, payload, HTTP_ACCEPT="application/json")
        result = response.json()
        self.assertEqual(result["target"], "#vinculos-documentales")
        self.assertNotIn("redirect", result)
        self.assertIn("Contrato folio 123", result["html"])
        self.assertEqual(VinculoProveedorDocumental.objects.count(), 1)
        UserModuleAccess.objects.filter(user=self.actor, module="maestros.proveedores").update(access="none")
        self.assertEqual(self.client.get(self.url).status_code, 403)
        UserModuleAccess.objects.filter(user=self.actor, module="maestros.proveedores").update(access="view")
        response = self.client.get(self.url)
        self.assertContains(response, "Contrato folio 123")
        self.assertNotContains(response, "data-async-action")
        self.assertEqual(self.client.post(self.url, payload).status_code, 403)


class VinculosConcurrencyTests(TransactionTestCase):
    def test_concurrent_different_actors_same_pair_is_idempotent(self):
        actors = [get_user_model().objects.create_superuser(username=f"actor{i}", password="test") for i in range(2)]
        perfil = ProveedorServicio.objects.create(nombre="Perfil concurrente")
        proveedor = Proveedor.objects.create(nombre="Comercial concurrente")
        def submit(actor_id):
            close_old_connections()
            try:
                actor = get_user_model().objects.get(pk=actor_id)
                vinculo, created = confirmar_vinculo(user=actor, perfil_id=perfil.pk, proveedor_id=proveedor.pk,
                                                     motivo="Mismo motivo", evidencia="Mismo folio", confirmado=True)
                return vinculo.pk, created, actor_id
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(submit, [actor.pk for actor in actors]))
        self.assertEqual(len({result[0] for result in results}), 1)
        self.assertEqual(sum(result[1] for result in results), 1)
        self.assertEqual(VinculoProveedorDocumental.objects.count(), 1)
        self.assertEqual(AuditLog.objects.filter(model="mantenimiento.VinculoProveedorDocumental").count(), 1)
        winner = next(result[2] for result in results if result[1])
        self.assertEqual(VinculoProveedorDocumental.objects.get().autor_id, winner)
