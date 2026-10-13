import calendar
from datetime import date, timedelta

from django.db import migrations, models


def completar_fechas_inequivocas(apps, schema_editor):
    Registro = apps.get_model('bonos_produccion', 'RegistroDiarioProduccion')
    for registro in Registro.objects.using(schema_editor.connection.alias).filter(fecha__isnull=True).select_related('bono__periodo').iterator():
        periodo = registro.bono.periodo
        if periodo.fecha_inicio and periodo.fecha_fin:
            inicio, fin = periodo.fecha_inicio, periodo.fecha_fin
        else:
            inicio = date(periodo.anio, periodo.mes, 1)
            fin = date(periodo.anio, periodo.mes, calendar.monthrange(periodo.anio, periodo.mes)[1])
        coincidencias = []
        fecha = inicio
        while fecha <= fin:
            if fecha.day == registro.dia:
                coincidencias.append(fecha)
            fecha += timedelta(days=1)
        if len(coincidencias) == 1:
            Registro.objects.using(schema_editor.connection.alias).filter(pk=registro.pk).update(fecha=coincidencias[0])


class Migration(migrations.Migration):
    dependencies = [('bonos_produccion', '0009_contexto_rrhh_bonos')]
    operations = [
        migrations.RunPython(completar_fechas_inequivocas, migrations.RunPython.noop),
        migrations.AlterUniqueTogether(name='registrodiarioproduccion', unique_together=set()),
        migrations.AlterModelOptions(name='registrodiarioproduccion', options={'ordering': ['fecha', 'dia']}),
        migrations.AddConstraint(model_name='registrodiarioproduccion', constraint=models.UniqueConstraint(fields=('bono', 'fecha'), name='bp_registro_bono_fecha_unique')),
        migrations.AddConstraint(model_name='registrodiarioproduccion', constraint=models.UniqueConstraint(fields=('bono', 'dia'), condition=models.Q(fecha__isnull=True), name='bp_registro_legacy_dia_unique')),
    ]
