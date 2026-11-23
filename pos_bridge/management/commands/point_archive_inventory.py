"""Bounded read-only eligibility and semantic verification for category XLS archives."""
import hashlib
import json
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from pathlib import Path
import re
import signal
import sys
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError, OutputWrapper
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, connections, transaction
from django.utils import timezone

from pos_bridge.models import PointSalesExtractionTask, PointSalesNormalized, PointSalesRawStaging, PointSalesDailyProductFact, PointProductHistoryImport, PointMonthlySalesOfficial
from pos_bridge.services.archive_access import ArchiveStore
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService

NAME = re.compile(r'[0-9]{8}_[0-9]{6}_point_sales_category_[A-Za-z0-9_-]+\.xls')


class BoundedText(StringIO):
    def __init__(self,limit):
        super().__init__();self.limit=limit;self.size=0

    def write(self,value):
        self.size+=len(value.encode('utf-8'))
        if self.size>self.limit:raise ValueError('inventory_output_limit')
        return super().write(value)


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
    # Report quality incidents stay recorded; only this source's integrity gates retirement.
    if PointSalesRawStaging.objects.filter(source_file=task.source_file).exclude(task_id__in=refs.values('id')).exists() or PointSalesNormalized.objects.filter(source_file=task.source_file).exclude(task_id__in=refs.values('id')).exists():
        return False
    if PointSalesDailyProductFact.objects.filter(source_file=task.source_file).exclude(source_hash=task.source_hash).exists():
        return False
    if PointSalesDailyProductFact.objects.filter(source_file=task.source_file).exclude(sync_job_id__in=refs.values('sync_job_id')).exists():
        return False
    # Category sales readers share archive recovery; other report domains remain excluded.
    if (PointProductHistoryImport.objects.filter(report_path=task.source_file).exists()
            or PointMonthlySalesOfficial.objects.filter(report_path=task.source_file).exists()):
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
        parser.add_argument('--stream',action='store_true')

    def stream(self):
        output=self.stdout;previous=signal.signal(signal.SIGALRM,signal.SIG_DFL)
        signal.alarm(0)
        try:
            for sequence in range(1,513):
                line=sys.stdin.buffer.readline(4097)
                if not line:return
                if len(line)>4096 or not line.endswith(b'\n'):
                    raise CommandError('inventory_stream_request_limit')
                try:
                    request=json.loads(line)
                    if (not isinstance(request,dict) or set(request)!={'id','args'}
                            or type(request['id']) is not int or request['id']!=sequence
                            or not isinstance(request['args'],list) or len(request['args'])!=2
                            or any(not isinstance(x,str) for x in request['args'])):
                        raise ValueError()
                    key,value=request['args'];options=dict(name=None,parse=None,after_id=0,stream=False)
                    if key=='--after-id' and re.fullmatch('[0-9]{1,19}',value) and int(value)<=9223372036854775807:
                        options['after_id']=int(value)
                    elif key in ('--name','--parse') and value:
                        options[key[2:]]=value
                    else:raise ValueError()
                except (ValueError,TypeError,UnicodeError):
                    raise CommandError('inventory_stream_request_invalid') from None
                signal.alarm(85)
                result=BoundedText(1048576);warnings=BoundedText(16384)
                try:
                    self.stdout=OutputWrapper(result)
                    with redirect_stdout(warnings),redirect_stderr(warnings):
                        self.handle(**options)
                    response=json.dumps({'id':sequence,'result':json.loads(result.getvalue())},separators=(',',':'))
                    if len(response.encode('utf-8'))+1>1048576:
                        raise ValueError('inventory_response_limit')
                except Exception:
                    raise CommandError('inventory_stream_request_failed') from None
                finally:
                    self.stdout=output
                    connections.close_all()
                    signal.alarm(0)
                output.write(response);output.flush()
        finally:
            self.stdout=output
            signal.alarm(0);signal.signal(signal.SIGALRM,previous)

    def handle(self,*args,**options):
        if options.get('stream'):
            return self.stream()
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
