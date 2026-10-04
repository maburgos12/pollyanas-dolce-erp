"""Recover only the original, explicitly authorized pair; never synchronize Point.

Run through manage.py shell after official deployment. Default is dry-run.
Original COPY values (including PK, writer and timestamps) remain unchanged.
"""
import gzip
import io
import json
import re
from decimal import Decimal

from django.db import connection, transaction
from psycopg2 import sql

from pos_bridge.services.product_month_source_mutex import lock_product_month_sources
from datetime import date


SOURCE_HASH = '9f27f6de945bbc8b19cd'
BACKUP = '/opt/backups/erp/backup_20261003_020001.sql.gz'
TABLES = ('control_mermapos', 'pos_bridge_waste_lines')


def _original_pair(backup):
    evidence, current = {}, None
    with gzip.open(backup, 'rt', encoding='utf-8') as stream:
        for line in stream:
            match = re.fullmatch(r'COPY public\.(\w+) \(([^\n]+)\) FROM stdin;\n', line)
            if match:
                table, fields = match.groups()
                current = (table, tuple(fields.split(', '))) if table in TABLES else None
                continue
            if line.rstrip('\n') == r'\.':
                current = None
                if len(evidence) == 2:
                    break
                continue
            if current is None:
                continue
            table, fields = current
            values = line.rstrip('\n').split('\t')
            if len(fields) != len(values):
                raise ValueError('Invalid COPY evidence width')
            data = dict(zip(fields, values))
            if data.get('source_hash') != SOURCE_HASH:
                continue
            if table in evidence or data.get('id') != '1676':
                raise ValueError('Duplicate or conflicting original identity')
            quantity = data.get('quantity', data.get('cantidad'))
            if Decimal(quantity) != Decimal('5') or data.get('receta_id') != '16':
                raise ValueError('Original product/quantity mismatch')
            if table == 'pos_bridge_waste_lines':
                # pg_dump COPY escapes JSON backslashes; this evidence has none.
                payload = json.loads(data['raw_payload'])
                if (data.get('movement_external_id') != '1683114'
                        or data.get('branch_id') != '24'
                        or data.get('erp_branch_id') != '1'
                        or data.get('sync_job_id') != '79066'
                        or data.get('insumo_id') != r'\N'
                        or data.get('unit') != 'PZA'
                        or data.get('movement_at') != '2026-09-27 19:01:00.49+00'
                        or payload.get('movement', {}).get('PK_Movimiento') != 1683114
                        or payload.get('movement', {}).get('Sucursal') != 'Matriz'):
                    raise ValueError('Original movement evidence mismatch')
                details = payload.get('details', [])
                if len(details) != 1 or Decimal(str(details[0].get('Cantidad'))) != 5:
                    raise ValueError('Original detail mismatch')
            elif (data.get('sucursal_id') != '1' or data.get('fecha') != '2026-09-27'
                    or data.get('codigo_point') != '0135'
                    or data.get('fuente') != 'POINT_BRIDGE_WASTE'):
                raise ValueError('Original ledger evidence mismatch')
            evidence[table] = (fields, line)
    if set(evidence) != set(TABLES):
        raise ValueError('Both original evidence rows are required')
    return evidence


def recover_original_waste(*, apply=False, backup=BACKUP):
    if connection.vendor != 'postgresql':
        raise ValueError('Recovery requires PostgreSQL')
    evidence = _original_pair(backup)
    restored, missing = 0, 0
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            lock_product_month_sources([date(2026, 9, 1)])
            # Serialize the exact pair against writers that do not honor the month mutex.
            cursor.execute('LOCK TABLE control_mermapos, pos_bridge_waste_lines IN SHARE ROW EXCLUSIVE MODE')
            for table in TABLES:
                fields, line = evidence[table]
                quoted_fields = sql.SQL(', ').join(map(sql.Identifier, fields))
                stage = 'recover_original_' + table
                cursor.execute(sql.SQL('CREATE TEMP TABLE {} ON COMMIT DROP AS SELECT {} FROM {} WITH NO DATA').format(
                    sql.Identifier(stage), quoted_fields, sql.Identifier(table)))
                copy = sql.SQL('COPY {} ({}) FROM STDIN').format(sql.Identifier(stage), quoted_fields)
                cursor.cursor.copy_expert(copy.as_string(connection.connection), io.StringIO(line))
                cursor.execute(sql.SQL('SELECT to_jsonb(s) FROM {} s').format(sql.Identifier(stage)))
                original = cursor.fetchone()[0]
                cursor.execute(sql.SQL('SELECT to_jsonb(t) FROM {} t WHERE id = 1676 OR source_hash = %s').format(sql.Identifier(table)), [SOURCE_HASH])
                existing = [row[0] for row in cursor.fetchall()]
                if existing:
                    if existing != [original]:
                        raise ValueError('Existing row conflicts with original evidence: ' + table)
                else:
                    missing += 1
                    if apply:
                        cursor.execute(sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {}').format(
                            sql.Identifier(table), quoted_fields, quoted_fields, sql.Identifier(stage)))
                        restored += cursor.rowcount
                cursor.execute(sql.SQL('DROP TABLE {}').format(sql.Identifier(stage)))
            # A failure above rolls back both inserts. No update, sequence reset,
            # sync-job rewrite, stock delta, notification or commercial write.
    return {'movement': '1683114', 'source_hash': SOURCE_HASH,
            'original_pk': 1676, 'would_restore': missing, 'restored': restored}
