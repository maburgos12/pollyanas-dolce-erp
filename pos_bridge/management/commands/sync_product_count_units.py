"""Refresh only counting-unit evidence; never writes to Point or stock."""
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointProduct
from pos_bridge.services.point_account_session_lock import point_account_session_lock
from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.services.product_count_units import COUNT_UNIT_KEY, catalog_count_unit


class Command(BaseCommand):
    help = 'Obtiene las unidades oficiales de Point para los conteos, sin modificar saldos.'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true')
        parser.add_argument('--branch', default='')

    def handle(self, *args, **options):
        with point_account_session_lock(wait=True), PointHttpSessionClient(load_point_bridge_settings()) as client:
            client.login(branch_hint=options['branch'] or None)
            units = {str(row['ID_Unidad']): row for row in client.get_units()}
            rows = client.get_all_products()
        evidence = {}
        for row in rows:
            unit = catalog_count_unit(row, units)
            if unit is None:
                raise CommandError(f"Producto Point sin unidad comprobable: {row.get('Codigo')}")
            code = unit['codigo']
            if code in evidence:
                raise CommandError(f'Código Point duplicado: {code}')
            evidence[code] = unit
        if not evidence:
            raise CommandError('Point no devolvió productos con unidad.')
        updated = 0
        with transaction.atomic():
            for product in PointProduct.objects.select_for_update().filter(active=True, sku__in=evidence):
                if not options['dry_run']:
                    product.metadata = {**(product.metadata or {}), COUNT_UNIT_KEY: evidence[product.sku]}
                    product.save(update_fields=['metadata'])
                updated += 1
        self.stdout.write(f"{'Simulación' if options['dry_run'] else 'Sincronizado'}: {updated} productos; {len(evidence)} códigos oficiales. Sin cambios a saldos ni conteos existentes.")
