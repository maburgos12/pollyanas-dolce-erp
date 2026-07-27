from datetime import timedelta

from django.contrib.auth.models import Group, User
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.access import ROLE_VENTAS
from core.models import Sucursal
from pos_bridge.models import PointBranch

from .models import Cliente, PedidoCliente, SeguimientoPedido
from .services import SucursalResolutionError, resolve_sucursal


class CRMViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="ventas", password="pass123")
        ventas_group, _ = Group.objects.get_or_create(name=ROLE_VENTAS)
        self.user.groups.add(ventas_group)
        self.client.login(username="ventas", password="pass123")
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.point_branch = PointBranch.objects.create(
            external_id="1",
            name="Matriz Point",
            erp_branch=self.sucursal,
            status=PointBranch.STATUS_ACTIVE,
        )

    def test_dashboard_view_renders_executive_surface(self):
        cliente = Cliente.objects.create(nombre="Cliente Dashboard")
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido dashboard",
            estatus=PedidoCliente.ESTATUS_EN_PRODUCCION,
            canal=PedidoCliente.CANAL_WHATSAPP,
            prioridad=PedidoCliente.PRIORIDAD_ALTA,
            monto_estimado=1250,
        )
        SeguimientoPedido.objects.create(
            pedido=pedido,
            estatus_nuevo=PedidoCliente.ESTATUS_EN_PRODUCCION,
            comentario="Producción confirmada",
            created_by=self.user,
        )

        resp = self.client.get(reverse("crm:home"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Clientes y pedidos en control")
        self.assertContains(resp, "Distribución por estatus")
        self.assertContains(resp, "Origen comercial")
        self.assertContains(resp, "Últimos 7 días")

    def test_dashboard_view_supports_real_status_and_query_filters(self):
        cliente = Cliente.objects.create(nombre="Cliente Filtrado", codigo="CLT-001")
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido filtrado",
            estatus=PedidoCliente.ESTATUS_EN_PRODUCCION,
            monto_estimado=800,
        )
        SeguimientoPedido.objects.create(
            pedido=pedido,
            estatus_nuevo=PedidoCliente.ESTATUS_EN_PRODUCCION,
            comentario="Filtro activo",
            created_by=self.user,
        )

        resp = self.client.get(
            reverse("crm:home"),
            {"estatus": PedidoCliente.ESTATUS_EN_PRODUCCION, "q": "Filtrado"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context["selected_estatus"], PedidoCliente.ESTATUS_EN_PRODUCCION)
        self.assertEqual(resp.context["selected_q"], "Filtrado")
        self.assertContains(resp, "Cliente o folio")

    def test_dashboard_view_supports_real_channel_and_priority_filters(self):
        cliente = Cliente.objects.create(nombre="Cliente Canal", codigo="CLT-CHAN-1")
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido canal",
            estatus=PedidoCliente.ESTATUS_CONFIRMADO,
            canal=PedidoCliente.CANAL_WHATSAPP,
            prioridad=PedidoCliente.PRIORIDAD_URGENTE,
            monto_estimado=900,
        )
        SeguimientoPedido.objects.create(
            pedido=pedido,
            estatus_nuevo=PedidoCliente.ESTATUS_CONFIRMADO,
            comentario="Seguimiento canal",
            created_by=self.user,
        )

        resp = self.client.get(
            reverse("crm:home"),
            {"canal": PedidoCliente.CANAL_WHATSAPP, "prioridad": PedidoCliente.PRIORIDAD_URGENTE},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context["selected_canal"], PedidoCliente.CANAL_WHATSAPP)
        self.assertEqual(resp.context["selected_prioridad"], PedidoCliente.PRIORIDAD_URGENTE)
        self.assertContains(resp, "Canal")
        self.assertContains(resp, "Prioridad")

    def test_clientes_view_and_create(self):
        resp = self.client.get(reverse("crm:clientes"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "CRM · Clientes")
        self.assertContains(resp, "Base comercial")
        self.assertContains(resp, "Alta de cliente")
        self.assertContains(resp, "Clientes registrados")
        self.assertNotContains(resp, "Centro de mando ERP")
        self.assertNotContains(resp, "Cadena documental ERP")
        self.assertNotContains(resp, "Ruta crítica ERP")
        self.assertNotContains(resp, "Mesa de gobierno ERP")
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertTrue(resp.context["operational_health_cards"])

        resp_post = self.client.post(
            reverse("crm:clientes"),
            {
                "nombre": "Cliente Demo",
                "telefono": "6671230000",
                "email": "demo@example.com",
                "tipo_cliente": "Mostrador",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertTrue(Cliente.objects.filter(nombre="Cliente Demo").exists())

    def test_cliente_detail_edit_deactivate_and_reactivate(self):
        cliente = Cliente.objects.create(
            nombre="Cliente Editable",
            telefono="6670000000",
            email="viejo@example.com",
            tipo_cliente="Mostrador",
        )
        PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido cliente editable",
            sucursal_ref=self.sucursal,
            monto_estimado=500,
        )

        detail_url = reverse("crm:cliente_detail", kwargs={"cliente_id": cliente.id})
        resp_detail = self.client.get(detail_url)
        self.assertEqual(resp_detail.status_code, 200)
        self.assertContains(resp_detail, "Cliente Editable")
        self.assertContains(resp_detail, "Historial de pedidos")
        self.assertContains(resp_detail, "Pedido cliente editable")

        edit_url = reverse("crm:editar_cliente", kwargs={"cliente_id": cliente.id})
        resp_edit = self.client.post(
            edit_url,
            {
                "accion": "editar",
                "nombre": "Cliente Editado",
                "telefono": "6671111111",
                "email": "nuevo@example.com",
                "tipo_cliente": "Evento",
                "sucursal_referencia": "Matriz",
                "notas": "Cliente actualizado desde test",
            },
            follow=True,
        )
        self.assertEqual(resp_edit.status_code, 200)
        cliente.refresh_from_db()
        self.assertEqual(cliente.nombre, "Cliente Editado")
        self.assertEqual(cliente.nombre_normalizado, "cliente editado")
        self.assertEqual(cliente.email, "nuevo@example.com")

        resp_deactivate = self.client.post(edit_url, {"accion": "desactivar"}, follow=True)
        self.assertEqual(resp_deactivate.status_code, 200)
        cliente.refresh_from_db()
        self.assertFalse(cliente.activo)

        resp_activate = self.client.post(edit_url, {"accion": "activar"}, follow=True)
        self.assertEqual(resp_activate.status_code, 200)
        cliente.refresh_from_db()
        self.assertTrue(cliente.activo)

    def test_pedidos_create_and_tracking(self):
        cliente = Cliente.objects.create(nombre="Cliente Pedido")
        resp_index = self.client.get(reverse("crm:pedidos"))
        self.assertEqual(resp_index.status_code, 200)
        self.assertContains(resp_index, "Nuevo pedido")
        self.assertContains(resp_index, "Seguimiento de pedidos")
        self.assertContains(resp_index, "Focos operativos")
        self.assertNotContains(resp_index, "Centro de mando ERP")
        self.assertNotContains(resp_index, "Cadena documental ERP")
        self.assertNotContains(resp_index, "Ruta crítica ERP")
        self.assertNotContains(resp_index, "Mesa de gobierno ERP")
        self.assertTrue(resp_index.context["focus_cards"])
        self.assertTrue(resp_index.context["enterprise_chain"])
        self.assertIn("dependency_status", resp_index.context["enterprise_chain"][0])
        self.assertTrue(resp_index.context["operational_health_cards"])
        resp_post = self.client.post(
            reverse("crm:pedidos"),
            {
                "cliente_id": cliente.id,
                "descripcion": "Pastel cumpleaños 30 personas",
                "sucursal": self.point_branch.external_id,
                "canal": PedidoCliente.CANAL_WHATSAPP,
                "prioridad": PedidoCliente.PRIORIDAD_ALTA,
                "estatus": PedidoCliente.ESTATUS_NUEVO,
                "monto_estimado": "1450.00",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)

        pedido = PedidoCliente.objects.get(cliente=cliente)
        self.assertEqual(pedido.estatus, PedidoCliente.ESTATUS_NUEVO)
        self.assertEqual(pedido.monto_estimado, 1450)
        self.assertEqual(pedido.sucursal_ref_id, self.sucursal.id)
        self.assertEqual(pedido.sucursal, self.sucursal.nombre)
        self.assertTrue(SeguimientoPedido.objects.filter(pedido=pedido).exists())

        detail_url = reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id})
        resp_detail = self.client.post(
            detail_url,
            {
                "estatus_nuevo": PedidoCliente.ESTATUS_CONFIRMADO,
                "comentario": "Cliente confirma anticipo",
            },
            follow=True,
        )
        self.assertEqual(resp_detail.status_code, 200)
        self.assertContains(resp_detail, "Ficha del pedido")
        self.assertContains(resp_detail, "Registrar seguimiento")
        self.assertContains(resp_detail, "Bitácora del pedido")
        self.assertNotContains(resp_detail, "Centro de mando ERP")
        self.assertNotContains(resp_detail, "Cadena documental ERP")
        self.assertNotContains(resp_detail, "Mesa de gobierno ERP")
        self.assertTrue(resp_detail.context["enterprise_chain"])
        self.assertIn("dependency_status", resp_detail.context["enterprise_chain"][0])
        self.assertTrue(resp_detail.context["operational_health_cards"])
        pedido.refresh_from_db()
        self.assertEqual(pedido.estatus, PedidoCliente.ESTATUS_CONFIRMADO)
        self.assertTrue(
            SeguimientoPedido.objects.filter(
                pedido=pedido,
                estatus_nuevo=PedidoCliente.ESTATUS_CONFIRMADO,
                comentario__icontains="anticipo",
            ).exists()
        )

    def test_pedidos_can_focus_operational_subset(self):
        cliente = Cliente.objects.create(nombre="Cliente Foco")
        PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido en producción",
            estatus=PedidoCliente.ESTATUS_EN_PRODUCCION,
        )

        resp = self.client.get(reverse("crm:pedidos"), {"enterprise_focus": "PRODUCCION"})
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Quitar foco")
        self.assertEqual(resp.context["enterprise_focus"], "PRODUCCION")
        self.assertIsNotNone(resp.context["focus_summary"])

    def test_redirects_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("crm:pedidos"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)


class PedidoClienteSaveTests(TestCase):
    def setUp(self):
        self.cliente = Cliente.objects.create(nombre="Cliente Save")
        self.sucursal_matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.sucursal_norte = Sucursal.objects.create(codigo="NORTE", nombre="Norte", activa=True)

    def test_create_fills_sucursal_from_ref_when_blank(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido nuevo",
            sucursal_ref=self.sucursal_matriz,
        )

        self.assertEqual(pedido.sucursal, "Matriz")

    def test_update_refreshes_sucursal_when_it_still_matches_previous_canonical_value(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido canonico",
            sucursal_ref=self.sucursal_matriz,
        )

        pedido.sucursal_ref = self.sucursal_norte
        pedido.save()
        pedido.refresh_from_db()

        self.assertEqual(pedido.sucursal, "Norte")

    def test_update_preserves_manual_sucursal_value(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido manual",
            sucursal_ref=self.sucursal_matriz,
            sucursal="Alias histórico",
        )

        pedido.descripcion = "Pedido manual actualizado"
        pedido.save()
        pedido.refresh_from_db()

        self.assertEqual(pedido.sucursal, "Alias histórico")

    def test_update_fields_persists_safe_fill_when_sucursal_is_blank(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido blank",
            sucursal_ref=self.sucursal_matriz,
        )
        PedidoCliente.objects.filter(pk=pedido.pk).update(sucursal="")

        pedido.refresh_from_db()
        pedido.estatus = PedidoCliente.ESTATUS_CONFIRMADO
        pedido.save(update_fields=["estatus", "updated_at"])
        pedido.refresh_from_db()

        self.assertEqual(pedido.sucursal, "Matriz")


class PedidoClientePointLinkTests(TestCase):
    def setUp(self):
        self.cliente = Cliente.objects.create(nombre="Cliente Point")
        self.otro_cliente = Cliente.objects.create(nombre="Otro cliente Point")

    def test_pedido_accepts_social_channels(self):
        self.assertIn(("FACEBOOK", "Facebook"), PedidoCliente.CANAL_CHOICES)
        self.assertIn(("INSTAGRAM", "Instagram"), PedidoCliente.CANAL_CHOICES)

    def test_point_note_identity_is_unique_when_present(self):
        PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Uno",
            point_note_id="900001",
            point_note_snapshot={"pk_nota": "900001"},
        )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                PedidoCliente.objects.create(
                    cliente=self.otro_cliente,
                    descripcion="Dos",
                    point_note_id="900001",
                    point_note_snapshot={"pk_nota": "900001"},
                )

    def test_empty_point_note_identity_is_not_unique(self):
        PedidoCliente.objects.create(cliente=self.cliente, descripcion="Uno")
        PedidoCliente.objects.create(cliente=self.otro_cliente, descripcion="Dos")

        self.assertEqual(PedidoCliente.objects.filter(point_note_id="").count(), 2)

    def test_point_note_snapshot_is_immutable(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Uno",
            point_note_id="900001",
            point_note_snapshot={"pk_nota": "900001"},
        )
        pedido.point_note_snapshot = {"pk_nota": "alterada"}

        with self.assertRaisesMessage(
            ValidationError,
            "La procedencia Point es inmutable.",
        ):
            pedido.save()

        pedido.refresh_from_db()
        self.assertEqual(pedido.point_note_snapshot, {"pk_nota": "900001"})

    def test_point_note_provenance_allows_explicit_legacy_backfill_once(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Legacy",
        )
        fetched_at = timezone.now()

        pedido.backfill_point_note_provenance(
            point_note_id="900001",
            point_note_folio="18452",
            point_note_snapshot={"pk_nota": "900001"},
            point_note_fetched_at=fetched_at,
        )
        pedido.refresh_from_db()

        self.assertEqual(pedido.point_note_id, "900001")
        self.assertEqual(pedido.point_note_folio, "18452")
        self.assertEqual(pedido.point_note_snapshot, {"pk_nota": "900001"})
        self.assertEqual(pedido.point_note_fetched_at, fetched_at)

        with self.assertRaisesMessage(ValidationError, "La procedencia Point es inmutable."):
            pedido.backfill_point_note_provenance(
                point_note_id="900001",
                point_note_folio="otro",
                point_note_snapshot={"pk_nota": "900001"},
                point_note_fetched_at=fetched_at,
            )

    def test_point_note_provenance_fields_are_immutable_as_a_set(self):
        fetched_at = timezone.now()
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Uno",
            point_note_id="900001",
            point_note_folio="18452",
            point_note_snapshot={"pk_nota": "900001"},
            point_note_fetched_at=fetched_at,
        )
        changes = {
            "point_note_id": "900002",
            "point_note_folio": "18453",
            "point_note_snapshot": {"pk_nota": "900001", "folio": "otro"},
            "point_note_fetched_at": fetched_at + timedelta(seconds=1),
        }

        for field_name, changed_value in changes.items():
            with self.subTest(field=field_name):
                pedido.refresh_from_db()
                setattr(pedido, field_name, changed_value)
                with self.assertRaisesMessage(ValidationError, "La procedencia Point es inmutable."):
                    pedido.save()

    def test_point_note_snapshot_identity_must_match_point_note_id(self):
        with self.assertRaisesMessage(ValidationError, "no coincide"):
            PedidoCliente.objects.create(
                cliente=self.cliente,
                descripcion="Mismatch",
                point_note_id="900001",
                point_note_snapshot={"pk_nota": "900002"},
            )

    def test_point_note_snapshot_accepts_real_uppercase_identity_key(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Contrato real",
            point_note_id="900001",
            point_note_snapshot={"PK_NOTA": "900001"},
        )

        self.assertEqual(pedido.point_note_id, "900001")

    def test_queryset_update_cannot_overwrite_immutable_snapshots_or_point_provenance(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Uno",
            point_note_id="900001",
            point_note_snapshot={"pk_nota": "900001"},
        )

        forbidden_updates = (
            {"payload_snapshot": {"alterado": True}},
            {"point_note_snapshot": {"pk_nota": "900002"}},
            {"point_note_id": "900002"},
            {"point_note_folio": "otro"},
            {"point_note_fetched_at": timezone.now()},
        )
        for update_kwargs in forbidden_updates:
            with self.subTest(fields=tuple(update_kwargs)):
                with self.assertRaisesMessage(ValidationError, "actualización masiva"):
                    PedidoCliente.objects.filter(pk=pedido.pk).update(**update_kwargs)

    def test_bulk_update_cannot_overwrite_immutable_snapshots_or_point_provenance(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Uno",
            point_note_id="900001",
            point_note_snapshot={"pk_nota": "900001"},
        )
        pedido.point_note_snapshot = {"pk_nota": "900002"}

        with self.assertRaisesMessage(ValidationError, "actualización masiva"):
            PedidoCliente.objects.bulk_update([pedido], ["point_note_snapshot"])

    def test_bulk_create_validates_point_note_identity(self):
        pedido = PedidoCliente(
            cliente=self.cliente,
            descripcion="Mismatch masivo",
            point_note_id="900001",
            point_note_snapshot={"PK_NOTA": "900002"},
        )

        with self.assertRaisesMessage(ValidationError, "no coincide"):
            PedidoCliente.objects.bulk_create([pedido])

    def test_payload_snapshot_remains_immutable_after_refactor(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Omnicanal",
            payload_snapshot={"external_id": "web-001"},
        )
        pedido.payload_snapshot = {"external_id": "alterado"}

        with self.assertRaisesMessage(
            ValidationError,
            "El snapshot omnicanal es inmutable.",
        ):
            pedido.save()

        pedido.refresh_from_db()
        self.assertEqual(pedido.payload_snapshot, {"external_id": "web-001"})

    def test_payload_snapshot_allows_legacy_backfill_only_once(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Omnicanal legacy",
        )
        pedido.payload_snapshot = {"external_id": "web-001"}
        pedido.save(_allow_payload_snapshot_backfill=True)
        pedido.refresh_from_db()
        self.assertEqual(pedido.payload_snapshot, {"external_id": "web-001"})

        pedido.payload_snapshot = {"external_id": "web-002"}
        with self.assertRaisesMessage(
            ValidationError,
            "El snapshot omnicanal es inmutable.",
        ):
            pedido.save(_allow_payload_snapshot_backfill=True)


class SucursalResolutionTests(TestCase):
    def test_resolve_sucursal_excludes_col_duplicate_and_keeps_canonical_branch(self):
        canonical = Sucursal.objects.create(codigo="COLOSIO", nombre="Colosio", activa=True)
        Sucursal.objects.create(codigo="COL", nombre="Colosio", activa=True)
        PointBranch.objects.create(
            external_id="5",
            name="COLOSIO",
            erp_branch=canonical,
            status=PointBranch.STATUS_ACTIVE,
        )

        resolution = resolve_sucursal("Colosio")

        self.assertEqual(resolution.sucursal.id, canonical.id)
        self.assertEqual(resolution.sucursal.codigo, "COLOSIO")

    def test_resolve_sucursal_rejects_excluded_duplicate_code(self):
        # El duplicado fantasma COL se remedia con datos (comando
        # purge_ghost_branch_col, 0ba49d89), no con la lista de exclusión del
        # resolver; el contrato vigente del resolver es rechazar códigos de
        # EXCLUDED_BRANCH_CODES (core.branch_catalog) aunque existan activos.
        Sucursal.objects.create(codigo="COLOSIO", nombre="Colosio", activa=True)
        Sucursal.objects.create(codigo="TMP1", nombre="Colosio", activa=True)

        with self.assertRaises(SucursalResolutionError):
            resolve_sucursal("TMP1")
