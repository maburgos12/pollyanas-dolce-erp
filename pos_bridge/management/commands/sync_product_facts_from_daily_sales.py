"""
Sincroniza PointSalesDailyProductFact desde PointDailySale para un rango de fechas.

Uso:
    python manage.py sync_product_facts_from_daily_sales
    python manage.py sync_product_facts_from_daily_sales --days 7
    python manage.py sync_product_facts_from_daily_sales --start 2026-04-20 --end 2026-05-28
"""
from __future__ import annotations

from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from pos_bridge.models import PointDailySale, PointSalesDailyProductFact, PointSalesNormalized


def sync_product_facts_for_range(start_date: date, end_date: date, stdout=None) -> int:
    current = start_date
    created_total = 0

    while current <= end_date:
        daily_sales = list(
            PointDailySale.objects
            .filter(sale_date=current, quantity__gt=0)
            .select_related("branch", "product", "receta", "sync_job")
        )
        if not daily_sales:
            current += timedelta(days=1)
            continue

        facts = []
        for sale in daily_sales:
            branch = sale.branch
            product = sale.product
            receta = sale.receta
            match_status = (
                PointSalesNormalized.MATCH_EXACT_CODE if receta
                else PointSalesNormalized.MATCH_SIN_CATALOGO
            )
            facts.append(PointSalesDailyProductFact(
                branch=branch,
                sync_job=sale.sync_job,
                sale_date=current,
                sucursal_nombre=branch.name if branch else "",
                categoria=product.category if product else "",
                producto_nombre_historico=product.name if product else "",
                point_product=product,
                receta=receta,
                match_catalogo_status=match_status,
                source_granularity=PointSalesDailyProductFact.GRANULARITY_PRODUCT,
                total_cantidad=sale.quantity,
                total_descuento=sale.discount_amount,
                total_venta=sale.total_amount,
                total_impuestos=sale.tax_amount,
                total_venta_neta=sale.net_amount,
            ))

        with transaction.atomic():
            PointSalesDailyProductFact.objects.filter(sale_date=current).delete()
            PointSalesDailyProductFact.objects.bulk_create(facts, batch_size=500)

        created_total += len(facts)
        if stdout:
            stdout.write(f"  {current}: {len(facts)} facts")
        current += timedelta(days=1)

    return created_total


class Command(BaseCommand):
    help = "Sincroniza PointSalesDailyProductFact desde PointDailySale (modo OFFICIAL)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--days",
            type=int,
            default=3,
            help="Cuantos dias hacia atras sincronizar (default: 3)",
        )
        parser.add_argument("--start", type=str, default=None, help="Fecha inicio YYYY-MM-DD")
        parser.add_argument("--end", type=str, default=None, help="Fecha fin YYYY-MM-DD")

    def handle(self, *args, **options):
        today = timezone.localdate()

        if options["start"] and options["end"]:
            start_date = date.fromisoformat(options["start"])
            end_date = date.fromisoformat(options["end"])
        else:
            end_date = today - timedelta(days=1)
            start_date = end_date - timedelta(days=options["days"] - 1)

        self.stdout.write(f"Sincronizando PointSalesDailyProductFact: {start_date} → {end_date}")
        total = sync_product_facts_for_range(start_date, end_date, stdout=self.stdout)
        self.stdout.write(self.style.SUCCESS(f"Listo: {total} facts creados/actualizados"))
