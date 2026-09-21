"""Small branch-specific counting scope; never returns expected quantities."""
from datetime import timedelta

from django.db.models import Q
from django.utils import timezone

from pos_bridge.models import (
    PointInventorySnapshot, PointInsumoInventorySnapshot,
    PointTransferLine, PointSyncJob,
)
from ventas.services.sales_read_service import sold_point_skus_for_range


def codigos_habituales(sucursal, *, ahora=None):
    if sucursal is None:
        return set()
    ahora = ahora or timezone.now()
    today = timezone.localdate(ahora)
    codes = sold_point_skus_for_range(
        sucursal=sucursal, start_date=today-timedelta(days=30), end_date=today,
    )
    receipts = PointTransferLine.objects.filter(
        Q(erp_destination_branch=sucursal)|Q(destination_branch__erp_branch=sucursal),
        is_received=True, is_cancelled=False, is_current_snapshot=True,
        received_quantity__gt=0, received_at__range=(ahora-timedelta(days=30), ahora))
    codes.update(receipts.order_by().values_list('item_code', flat=True).distinct())
    for model, quantity, code in (
        (PointInventorySnapshot, 'stock', 'product__sku'),
        (PointInsumoInventorySnapshot, 'point_quantity', 'point_code'),
    ):
        recent = model.objects.filter(
            branch__erp_branch=sucursal,
            captured_at__range=(ahora-timedelta(days=7), ahora),
            sync_job__status=PointSyncJob.STATUS_SUCCESS,
            sync_job__job_type=PointSyncJob.JOB_TYPE_INVENTORY)
        # Choose the latest completed cycle first, then inspect its positive
        # rows. Filtering positive stock before selecting the cycle resurrects
        # old seasonal stock that is zero in the latest extraction.
        cycles = recent.order_by('branch_id', '-captured_at', '-pk').distinct('branch_id').values('branch_id', 'sync_job_id')
        for cycle in cycles:
            codes.update(recent.filter(**cycle, **{quantity+'__gt':0}).order_by().values_list(code, flat=True))
    return {str(code).strip() for code in codes if code and str(code).strip()}
