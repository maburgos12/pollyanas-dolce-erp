import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("pos_bridge", "0023_enforce_open_transfer_snapshot_append_only")]

    operations = [
        migrations.CreateModel(
            name="PointInventoryPayload",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("digest", models.CharField(max_length=64, unique=True)),
                ("payload", models.JSONField()),
            ],
            options={"db_table": "pos_bridge_inventory_payloads"},
        ),
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    """
                    SET LOCAL lock_timeout = '3s';
                    ALTER TABLE pos_bridge_inventory_snapshots ADD COLUMN shared_payload_id bigint;
                    ALTER TABLE pos_bridge_inventory_snapshots
                      ADD CONSTRAINT pb_inv_shared_payload_fk
                      FOREIGN KEY (shared_payload_id) REFERENCES pos_bridge_inventory_payloads (id) NOT VALID;
                    """,
                ),
            ],
            state_operations=[
                migrations.AddField(
                    model_name="pointinventorysnapshot",
                    name="shared_payload",
                    field=models.ForeignKey(
                        db_index=False, editable=False, null=True,
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="+", to="pos_bridge.pointinventorypayload",
                    ),
                ),
            ],
        ),
        migrations.RunSQL(
            """
            CREATE FUNCTION pb_inventory_payload_immutable() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'Shared Point inventory evidence is immutable';
            END;
            $$;
            CREATE TRIGGER pb_inventory_payload_immutable
            BEFORE UPDATE ON pos_bridge_inventory_payloads
            FOR EACH ROW EXECUTE FUNCTION pb_inventory_payload_immutable();
            """,
            reverse_sql="""
            DROP TRIGGER pb_inventory_payload_immutable ON pos_bridge_inventory_payloads;
            DROP FUNCTION pb_inventory_payload_immutable();
            """,
        ),
    ]
