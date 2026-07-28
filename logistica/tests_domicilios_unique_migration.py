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

    def tearDown(self):
        executor = MigrationExecutor(connection)
        if (
            "logistica",
            "0047_solicituddomicilio_pedido_unico",
        ) not in executor.loader.applied_migrations:
            apps = executor.loader.project_state(self.migrate_from).apps
            Solicitud = apps.get_model("logistica", "SolicitudDomicilio")
            linked_ids = list(
                Solicitud.objects.exclude(pedido_cliente_id=None)
                .order_by("id")
                .values_list("id", flat=True)
            )
            if len(linked_ids) > 1:
                Solicitud.objects.filter(id__in=linked_ids[1:]).update(
                    pedido_cliente_id=None,
                )
        executor.migrate(executor.loader.graph.leaf_nodes())
        super().tearDown()

    def _at_0046(self):
        executor = MigrationExecutor(connection)
        executor.migrate(self.migrate_from)
        return executor.loader.project_state(self.migrate_from).apps

    def _fixtures(self, apps, *, duplicate):
        Cliente = apps.get_model("crm", "Cliente")
        Pedido = apps.get_model("crm", "PedidoCliente")
        Solicitud = apps.get_model("logistica", "SolicitudDomicilio")
        cliente = Cliente.objects.create(nombre="Preflight 0047")
        pedido = Pedido.objects.create(
            cliente=cliente,
            descripcion="Pedido preflight",
            point_note_id="POINT-PREFLIGHT-0047",
            point_note_folio="PREFLIGHT-0047",
            point_note_snapshot={"pk_nota": "POINT-PREFLIGHT-0047"},
            point_link_fingerprint="fingerprint-test",
        )
        first = Solicitud.objects.create(
            pedido_cliente=pedido,
            cliente_nombre="Primero",
            direccion="Dirección original",
            estatus="CONFIRMADO",
            notas="Evidencia uno",
        )
        second = None
        if duplicate:
            second = Solicitud.objects.create(
                pedido_cliente=pedido,
                cliente_nombre="Segundo",
                direccion="Dirección duplicada",
                estatus="ENTREGADO",
                notas="Evidencia dos",
            )
        return pedido, first, second

    def test_zero_duplicates_applies_constraint_without_mutating_data(self):
        old_apps = self._at_0046()
        pedido, first, _ = self._fixtures(old_apps, duplicate=False)

        executor = MigrationExecutor(connection)
        executor.migrate(self.migrate_to)
        new_apps = executor.loader.project_state(self.migrate_to).apps
        Solicitud = new_apps.get_model("logistica", "SolicitudDomicilio")
        preserved = Solicitud.objects.get(pk=first.pk)
        self.assertEqual(preserved.pedido_cliente_id, pedido.pk)
        self.assertEqual(preserved.notas, "Evidencia uno")
        with self.assertRaises(IntegrityError), transaction.atomic():
            Solicitud.objects.create(
                pedido_cliente_id=pedido.pk,
                cliente_nombre="Impedido",
                direccion="No se inserta",
                estatus="CONFIRMADO",
            )

    def test_duplicates_abort_atomically_without_schema_or_data_changes(self):
        old_apps = self._at_0046()
        pedido, first, second = self._fixtures(old_apps, duplicate=True)

        with self.assertRaisesRegex(
            RuntimeError,
            r"no elige, cancela ni modifica registros automáticamente",
        ):
            MigrationExecutor(connection).migrate(self.migrate_to)

        executor = MigrationExecutor(connection)
        self.assertNotIn(
            ("logistica", "0047_solicituddomicilio_pedido_unico"),
            executor.loader.applied_migrations,
        )
        apps = executor.loader.project_state(self.migrate_from).apps
        Solicitud = apps.get_model("logistica", "SolicitudDomicilio")
        rows = list(
            Solicitud.objects.filter(pedido_cliente_id=pedido.pk)
            .order_by("id")
            .values_list("id", "estatus", "notas")
        )
        self.assertEqual(
            rows,
            [
                (first.pk, "CONFIRMADO", "Evidencia uno"),
                (second.pk, "ENTREGADO", "Evidencia dos"),
            ],
        )
        columns = {
            column.name
            for column in connection.introspection.get_table_description(
                connection.cursor(),
                "logistica_solicituddomicilio",
            )
        }
        self.assertNotIn("duplicado_de_id", columns)
        self.assertNotIn("pedido_cliente_original_id", columns)
