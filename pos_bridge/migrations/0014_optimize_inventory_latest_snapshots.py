from django.db import migrations, models


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("pos_bridge", "0013_pointproductcategory"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pb_inv_latest_idx "
                        "ON pos_bridge_inventory_snapshots "
                        "(branch_id, product_id, captured_at DESC, id DESC)"
                    ),
                    reverse_sql="DROP INDEX CONCURRENTLY IF EXISTS pb_inv_latest_idx",
                ),
            ],
            state_operations=[
                migrations.AddIndex(
                    model_name="pointinventorysnapshot",
                    index=models.Index(
                        fields=["branch", "product", "-captured_at", "-id"],
                        name="pb_inv_latest_idx",
                    ),
                ),
            ],
        ),
    ]
