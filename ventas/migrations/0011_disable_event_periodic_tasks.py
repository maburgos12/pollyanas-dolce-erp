from django.db import migrations
from django.db.models import Q


def disable_event_periodic_tasks(apps, schema_editor):
    PeriodicTask = apps.get_model("django_celery_beat", "PeriodicTask")
    event_tasks = PeriodicTask.objects.filter(
        Q(task__icontains="event")
        | Q(task__icontains="evento")
        | Q(name__icontains="event")
        | Q(name__icontains="evento")
    )
    for task in event_tasks:
        task.enabled = False
        task.save(update_fields=["enabled"])


class Migration(migrations.Migration):

    dependencies = [
        ("ventas", "0010_eliminar_modelos_eventos"),
    ]

    operations = [
        migrations.RunPython(disable_event_periodic_tasks, migrations.RunPython.noop),
    ]
