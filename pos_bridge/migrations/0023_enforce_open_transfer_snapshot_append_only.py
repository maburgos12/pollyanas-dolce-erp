from django.db import migrations


CREATE_APPEND_ONLY_GUARD = """
CREATE OR REPLACE FUNCTION pos_bridge_reject_open_transfer_snapshot_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'La evidencia histórica de cierre es inmutable.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pb_open_snapshot_immutable
BEFORE UPDATE OR DELETE ON pos_bridge_open_transfer_snapshots
FOR EACH ROW EXECUTE FUNCTION pos_bridge_reject_open_transfer_snapshot_mutation();

CREATE TRIGGER pb_open_snapshot_member_immutable
BEFORE UPDATE OR DELETE ON pos_bridge_open_transfer_snapshot_members
FOR EACH ROW EXECUTE FUNCTION pos_bridge_reject_open_transfer_snapshot_mutation();
"""


DROP_APPEND_ONLY_GUARD = """
DROP TRIGGER IF EXISTS pb_open_snapshot_member_immutable
ON pos_bridge_open_transfer_snapshot_members;
DROP TRIGGER IF EXISTS pb_open_snapshot_immutable
ON pos_bridge_open_transfer_snapshots;
DROP FUNCTION IF EXISTS pos_bridge_reject_open_transfer_snapshot_mutation();
"""


class Migration(migrations.Migration):
    dependencies = [
        ("pos_bridge", "0022_immutable_open_transfer_snapshots"),
    ]

    operations = [
        migrations.RunSQL(
            sql=CREATE_APPEND_ONLY_GUARD,
            reverse_sql=DROP_APPEND_ONLY_GUARD,
        ),
    ]
