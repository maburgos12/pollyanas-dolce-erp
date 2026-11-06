import json
from django.core.management.base import BaseCommand, CommandError
from inventario.services_documentary_audit import month
from inventario.services_reconciliation_agent import InsumoReconciliationAgent

class Command(BaseCommand):
    help='Investiga compras, transferencias y cobertura. --guardar persiste expedientes y solicitudes; nunca ajusta inventario.'
    def add_arguments(self,parser):
        parser.add_argument('--mes',required=True)
        parser.add_argument('--guardar',action='store_true')
    def handle(self,*args,**options):
        try:
            period=month(options['mes'])
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        result=InsumoReconciliationAgent().run(period,dry_run=not options['guardar'])
        self.stdout.write(json.dumps(result,ensure_ascii=False,default=str))
