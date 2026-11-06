from django.db import migrations
NAME='Conciliador de insumos (cada dos horas)'
TASK='inventario.conciliar_documentos_insumos'

def schedule(apps,schema_editor):
    cron,_=apps.get_model('django_celery_beat','CrontabSchedule').objects.get_or_create(
        minute='15',hour='7,9,11,13,15,17,19',day_of_week='*',day_of_month='*',month_of_year='*',timezone='America/Mazatlan')
    apps.get_model('django_celery_beat','PeriodicTask').objects.get_or_create(name=NAME,defaults={
        'task':TASK,'crontab':cron,'enabled':True,
        'description':'Investiga documentos y crea solicitudes internas idempotentes. No corrige inventario ni envía comunicaciones.'})

def disable(apps,schema_editor):
    apps.get_model('django_celery_beat','PeriodicTask').objects.filter(name=NAME,task=TASK).update(enabled=False)

class Migration(migrations.Migration):
    dependencies=[('inventario','0019_agente_conciliacion'),('django_celery_beat','0019_alter_periodictasks_options')]
    operations=[migrations.RunPython(schedule,disable)]
