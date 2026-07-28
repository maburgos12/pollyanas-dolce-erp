from django.db import IntegrityError, connection, transaction
from django.db.migrations.executor import MigrationExecutor
from django.test import TransactionTestCase


class SolicitudDomicilioUniqueOrderMigrationTests(TransactionTestCase):
    migrate_from = [
        ("crm", "0008_point_link_fingerprint_indexes"),
        ("logistica", "0046_solicituddomicilio_operacion_canonica"),
    ]
    migrate_to = [
        ("crm", "0008_point_link_fingerprint_indexes"),
        ("logistica", "0047_solicituddomicilio_pedido_unico"),
    ]

    def setUp(self):
        super().setUp()
        executor = MigrationExecutor(connection)
        executor.migrate(self.migrate_from)
        old_apps = executor.loader.project_state(self.migrate_from).apps

        Cliente = old_apps.get_model("crm", "Cliente")
        Pedido = old_apps.get_model("crm", "PedidoCliente")
        Solicitud = old_apps.get_model("logistica", "SolicitudDomicilio")
        cliente = Cliente.objects.create(nombre="Cliente duplicado histórico")
        pedido = Pedido.objects.create(
            cliente=cliente,
            descripcion="Nota Point duplicada histórica",
            point_note_id="POINT-DUP-0047",
            point_note_folio="DUP-0047",
            point_note_snapshot={"pk_nota": "POINT-DUP-0047", "total": "100.00"},
        )
        self.pedido_id = pedido.pk
        self.original = {}
        for key, status, notes, motive in (
            ("canonical", "CONFIRMADO", "Primera captura", ""),
            ("active_duplicate", "EN_RUTA", "Segunda captura", ""),
            ("terminal_duplicate", "ENTREGADO", "Entrega histórica", "Motivo previo"),
        ):
            row = Solicitud.objects.create(
                pedido_cliente=pedido,
                cliente_nombre=key,
                direccion="Dirección histórica",
                estatus=status,
                notas=notes,
                cancelacion_motivo=motive,
            )
            self.original[key] = {
                "id": row.pk,
                "pedido_cliente_id": row.pedido_cliente_id,
                "estatus": row.estatus,
                "legacy_without_point": row.legacy_without_point,
                "cancelacion_motivo": row.cancelacion_motivo,
                "notas": row.notas,
            }
        self.orphan_id = Solicitud.objects.create(
            cliente_nombre="Sin pedido",
            direccion="Dirección sin Point",
            estatus="PENDIENTE_POINT",
        ).pk

        executor = MigrationExecutor(connection)
        executor.migrate(self.migrate_to)
        self.new_apps = executor.loader.project_state(self.migrate_to).apps

    def tearDown(self):
        executor = MigrationExecutor(connection)
        executor.migrate(executor.loader.graph.leaf_nodes())
        super().tearDown()

    def test_forward_preserves_rows_and_marks_reconciled_duplicates(self):
        Solicitud = self.new_apps.get_model("logistica", "SolicitudDomicilio")
        canonical = Solicitud.objects.get(pk=self.original["canonical"]["id"])
        active = Solicitud.objects.get(pk=self.original["active_duplicate"]["id"])
        terminal = Solicitud.objects.get(pk=self.original["terminal_duplicate"]["id"])

        self.assertEqual(Solicitud.objects.count(), 4)
        self.assertEqual(canonical.pedido_cliente_id, self.pedido_id)
        self.assertIsNone(canonical.duplicado_de_id)
        self.assertIsNone(active.pedido_cliente_id)
        self.assertEqual(active.pedido_cliente_original_id, self.pedido_id)
        self.assertEqual(active.duplicado_de_id, canonical.pk)
        self.assertTrue(active.legacy_without_point)
        self.assertEqual(active.estatus, "CANCELADO")
        self.assertIn("Conciliación 0047", active.cancelacion_motivo)
        self.assertIn("ROLLBACK_DOMICILIO_0047:", active.notas)
        self.assertIsNone(terminal.pedido_cliente_id)
        self.assertEqual(terminal.pedido_cliente_original_id, self.pedido_id)
        self.assertEqual(terminal.duplicado_de_id, canonical.pk)
        self.assertEqual(terminal.estatus, "ENTREGADO")
        self.assertEqual(terminal.cancelacion_motivo, "Motivo previo")
        self.assertEqual(
            Solicitud.objects.get(pk=self.orphan_id).pedido_cliente_id,
            None,
        )

    def test_database_allows_null_orders_but_rejects_second_linked_delivery(self):
        Solicitud = self.new_apps.get_model("logistica", "SolicitudDomicilio")
        Solicitud.objects.create(
            cliente_nombre="Otro histórico sin pedido",
            direccion="Sin Point",
            estatus="PENDIENTE_POINT",
        )
        with self.assertRaises(IntegrityError), transaction.atomic():
            Solicitud.objects.create(
                pedido_cliente_id=self.pedido_id,
                cliente_nombre="Duplicado impedido",
                direccion="Con Point",
                estatus="CONFIRMADO",
            )

    def test_reverse_restores_exact_original_relationship_and_evidence(self):
        executor = MigrationExecutor(connection)
        executor.migrate(self.migrate_from)
        old_apps = executor.loader.project_state(self.migrate_from).apps
        Solicitud = old_apps.get_model("logistica", "SolicitudDomicilio")

        for expected in self.original.values():
            restored = Solicitud.objects.get(pk=expected["id"])
            self.assertEqual(restored.pedido_cliente_id, expected["pedido_cliente_id"])
            self.assertEqual(restored.estatus, expected["estatus"])
            self.assertEqual(restored.legacy_without_point, expected["legacy_without_point"])
            self.assertEqual(restored.cancelacion_motivo, expected["cancelacion_motivo"])
            self.assertEqual(restored.notas, expected["notas"])
