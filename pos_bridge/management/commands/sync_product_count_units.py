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
        ambiguous = set()
        for row in rows:
            unit = catalog_count_unit(row, units)
            if unit is None:
                raise CommandError(f"Producto Point sin unidad comprobable: {row.get('Codigo')}")
            code = unit['codigo']
            if code in evidence:
                ambiguous.add(code)
            evidence[code] = unit
        for code in ambiguous:
            evidence.pop(code, None)
        if not evidence:
            raise CommandError('Point no devolvió productos con unidad.')
        updated = 0
        with transaction.atomic():
            for product in PointProduct.objects.select_for_update().filter(active=True, sku__in=set(evidence)|ambiguous):
                if not options['dry_run']:
                    metadata = dict(product.metadata or {})
                    if product.sku in ambiguous:
                        metadata.pop(COUNT_UNIT_KEY, None)
                    else:
                        metadata[COUNT_UNIT_KEY] = evidence[product.sku]
                    if metadata != product.metadata:
                        product.metadata = metadata
                        product.save(update_fields=['metadata'])
                updated += product.sku not in ambiguous
        if ambiguous:
            self.stdout.write(self.style.WARNING('Códigos ambiguos sin unidad asignada: ' + ', '.join(sorted(ambiguous))))
        self.stdout.write(f"{'Simulación' if options['dry_run'] else 'Sincronizado'}: {updated} productos; {len(evidence)} códigos oficiales. Sin cambios a saldos ni conteos existentes.")
