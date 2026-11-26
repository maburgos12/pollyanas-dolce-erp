from django.db import connection, transaction

from pos_bridge.utils.exceptions import PersistenceError


_DOCUMENTS = """
    SELECT ordinal, payload::jsonb::text AS canonical
    FROM unnest(%s::text[]) WITH ORDINALITY AS inputs(payload, ordinal)
"""
_DIGEST = "encode(sha256(convert_to(canonical, 'UTF8')), 'hex')"


@transaction.atomic
def store_exact_inventory_payloads(documents):
    """Return aligned IDs without round-tripping historical JSON through Python."""
    if not documents:
        return []
    with connection.cursor() as cursor:
        cursor.execute(f"""
            WITH docs AS ({_DOCUMENTS})
            INSERT INTO pos_bridge_inventory_payloads (digest, payload)
            SELECT DISTINCT {_DIGEST}, canonical::jsonb FROM docs
            ORDER BY 1
            ON CONFLICT (digest) DO NOTHING
        """, [documents])
        cursor.execute(f"""
            WITH docs AS ({_DOCUMENTS}),
            keyed AS (SELECT ordinal, canonical, {_DIGEST} AS digest FROM docs)
            SELECT stored.id
            FROM keyed
            JOIN pos_bridge_inventory_payloads stored USING (digest)
            WHERE stored.payload::text = keyed.canonical
            ORDER BY keyed.ordinal
        """, [documents])
        payload_ids = [row[0] for row in cursor.fetchall()]
    if len(payload_ids) != len(documents):
        raise PersistenceError("Conflicto de huella en evidencia Point.")
    return payload_ids
