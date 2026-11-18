"""Bounded read-only eligibility and semantic verification for category XLS archives."""
import hashlib
import json
from pathlib import Path
import re
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, transaction
from django.db.models import Q
from django.utils import timezone

from pos_bridge.models import PointSalesExtractionTask, PointSalesNormalized, PointSalesRawStaging, PointSalesQualityAlert, PointSalesDailyProductFact, PointProductHistoryImport, PointMonthlySalesOfficial
from ventas.models import VentaAutoritativaPoint
from pos_bridge.services.archive_access import ArchiveStore
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService

NAME = re.compile(r'[0-9]{8}_[0-9]{6}_point_sales_category_[A-Za-z0-9_-]+\.xls')


def semantic_digest(path):
    parsed = PointSalesCategoryReportService().parse_report(report_path=str(path))
    content = json.dumps({'rows':parsed.rows,'summary':parsed.summary}, sort_keys=True, cls=DjangoJSONEncoder, separators=(',',':'))
    return {'semantic_sha256':hashlib.sha256(content.encode()).hexdigest(), 'rows':len(parsed.rows)}


def eligible_task(task):
    if task.status != 'SUCCESS' or task.sync_job.status != 'SUCCESS' or not task.finished_at or not task.sync_job.finished_at or task.row_count <= 0:
        return False
    if task.finished_at > timezone.now() - timedelta(days=30):
        return False
    refs = PointSalesExtractionTask.objects.filter(source_file=task.source_file)
    if refs.exclude(status='SUCCESS').exists() or refs.exclude(sync_job__status='SUCCESS').exists() or refs.exclude(source_hash=task.source_hash).exists():
        return False
    if PointSalesExtractionTask.objects.filter(branch_id=task.branch_id,sale_date=task.sale_date).exclude(status='SUCCESS').exists():
        return False
    if PointSalesQualityAlert.objects.filter(Q(task_id__in=refs.values('id')) | Q(sync_job_id__in=refs.values('sync_job_id')) | Q(branch_id=task.branch_id,fecha=task.sale_date)).exclude(severity='INFO').exists():
        return False
    if PointSalesRawStaging.objects.filter(source_file=task.source_file).exclude(task_id__in=refs.values('id')).exists() or PointSalesNormalized.objects.filter(source_file=task.source_file).exclude(task_id__in=refs.values('id')).exists():
        return False
    if PointSalesDailyProductFact.objects.filter(source_file=task.source_file).exclude(source_hash=task.source_hash).exists():
        return False
    if PointSalesDailyProductFact.objects.filter(source_file=task.source_file).exclude(sync_job_id__in=refs.values('sync_job_id')).exists():
        return False
    # Pilot excludes mixed-domain references until their closure contract is verified.
    if (PointProductHistoryImport.objects.filter(report_path=task.source_file).exists()
            or PointMonthlySalesOfficial.objects.filter(report_path=task.source_file).exists()
            or VentaAutoritativaPoint.objects.filter(source_file=task.source_file).exists()):
        return False
    for other in refs:
        if not other.finished_at or other.finished_at > timezone.now()-timedelta(days=30) or not other.sync_job.finished_at or other.row_count<=0:
            return False
        if PointSalesRawStaging.objects.filter(task=other).exclude(source_file=task.source_file,source_hash=task.source_hash).exists() or PointSalesNormalized.objects.filter(task=other).exclude(source_file=task.source_file,source_hash=task.source_hash).exists():
            return False
        if PointSalesRawStaging.objects.filter(task=other,source_hash=task.source_hash).count() != other.row_count:
            return False
        if PointSalesNormalized.objects.filter(task=other,source_hash=task.source_hash).count() != other.row_count:
            return False
    return True


class Command(BaseCommand):
    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument('--after-id',type=int,default=0)
        parser.add_argument('--name')
        parser.add_argument('--parse')

    def handle(self,*args,**options):
        if connection.vendor != 'postgresql':
            raise CommandError('PostgreSQL required')
        store=ArchiveStore()
        if options['parse']:
            path=Path(options['parse']).absolute()
            allowed=(store.sources_root,store.root/'staging')
            if path.parent not in allowed or path.suffix!='.xls' or path.is_symlink():
                raise CommandError('parse path outside allowed roots')
            self.stdout.write(json.dumps(semantic_digest(path)))
            return
        name=options['name']
        if name is not None and not NAME.fullmatch(name):
            raise CommandError('unsupported category source')
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute('SET TRANSACTION READ ONLY')
                cursor.execute('SET LOCAL statement_timeout=10000')
            query=PointSalesExtractionTask.objects.select_related('sync_job').filter(id__gt=options['after_id'],status='SUCCESS',sync_job__status='SUCCESS',finished_at__lte=timezone.now()-timedelta(days=30)).order_by('id')
            if name:
                query=query.filter(source_file=str(store.sources_root/name))
            tasks=list(query[:100]);entries=[];seen=set()
            for task in tasks:
                path=Path(task.source_file)
                if path.parent!=store.sources_root or not NAME.fullmatch(path.name) or path.name in seen or not eligible_task(task):
                    continue
                seen.add(path.name)
                if name:
                    # The host executor holds EX lease. Do not reacquire it in this subprocess.
                    try:
                        entries.append(dict(relative_path=path.name,task_id=task.id,sha256=task.source_hash,bytes=path.stat().st_size))
                    except FileNotFoundError:
                        entries.append(dict(relative_path=path.name,task_id=task.id,sha256=task.source_hash,bytes=None))
                    continue
                try:
                    with store.path_lock(path.name):
                        snap=store.snapshot(path)
                        if snap['sha256']!=task.source_hash or snap['bytes']>100_000_000:
                            continue
                        semantic=semantic_digest(path)
                except (OSError,RuntimeError,ValueError):
                    continue
                if semantic['rows']!=task.row_count:
                    continue
                entries.append(dict(relative_path=path.name,task_id=task.id,**snap,**semantic))
            self.stdout.write(json.dumps({'entries':entries,'next_id':tasks[-1].id if tasks else 0,'has_more':len(tasks)==100}))
