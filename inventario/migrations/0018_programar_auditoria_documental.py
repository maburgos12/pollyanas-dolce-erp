from django.db import migrations

NAME = 'Auditoría documental de compras e insumos (09:00)'
TASK = 'inventario.auditar_compras_produccion'


def schedule(apps, schema_editor):
    Schedule = apps.get_model('django_celery_beat', 'CrontabSchedule')
    Task = apps.get_model('django_celery_beat', 'PeriodicTask')
    crontab, _ = Schedule.objects.get_or_create(
        minute='0', hour='9', day_of_week='*', day_of_month='*', month_of_year='*',
        timezone='America/Mazatlan',
    )
    Task.objects.get_or_create(name=NAME, defaults={
        'task': TASK, 'crontab': crontab, 'enabled': True,
        'description': 'Revisión documental del mes actual y anterior. Sin ajustes ni avisos. Difiere si hay importaciones activas.',
    })


def disable(apps, schema_editor):
    apps.get_model('django_celery_beat', 'PeriodicTask').objects.filter(name=NAME, task=TASK).update(enabled=False)


class Migration(migrations.Migration):
    dependencies = [
        ('inventario', '0017_auditoria_documental'),
        ('django_celery_beat', '0019_alter_periodictasks_options'),
    ]
    operations = [migrations.RunPython(schedule, disable)]
