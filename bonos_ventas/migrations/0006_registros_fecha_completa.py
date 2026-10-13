import calendar
from datetime import date, timedelta
from django.db import migrations, models


def completar_fechas_seguras(apps, schema_editor):
    Registro = apps.get_model('bonos_ventas', 'RegistroDiarioVentas')
    registros = Registro.objects.using(schema_editor.connection.alias).filter(fecha__isnull=True).select_related('bono__periodo')
    for registro in registros.iterator():
        periodo = registro.bono.periodo
        if periodo.fecha_inicio and periodo.fecha_fin and periodo.fecha_inicio <= periodo.fecha_fin:
            inicio, fin = periodo.fecha_inicio, periodo.fecha_fin
        else:
            inicio = date(periodo.anio, periodo.mes, 1)
            fin = date(periodo.anio, periodo.mes, calendar.monthrange(periodo.anio, periodo.mes)[1])
        try:
            fecha = date(periodo.anio, periodo.mes, registro.dia)
        except ValueError:
            continue
        if not inicio <= fecha <= fin:
            continue
        # La sincronización RRHH usaba día del mes. Capturas con otra interpretación permanecen sin fecha.
        if registro.capturado_por_id and inicio + timedelta(days=registro.dia - 1) != fecha:
            continue
        Registro.objects.using(schema_editor.connection.alias).filter(pk=registro.pk).update(fecha=fecha)


class Migration(migrations.Migration):
    dependencies = [('bonos_ventas', '0005_bonoventasempleado_cancela_bono_and_more')]
    operations = [
        migrations.AddField(model_name='registrodiarioventas', name='fecha', field=models.DateField(null=True, blank=True)),
        migrations.RunPython(completar_fechas_seguras, migrations.RunPython.noop),
        migrations.AlterUniqueTogether(name='registrodiarioventas', unique_together=set()),
        migrations.AlterModelOptions(name='registrodiarioventas', options={'ordering':['fecha','dia']}),
        migrations.AddConstraint(model_name='registrodiarioventas', constraint=models.UniqueConstraint(fields=('bono','fecha'), name='bv_registro_bono_fecha_unique')),
        migrations.AddConstraint(model_name='registrodiarioventas', constraint=models.UniqueConstraint(fields=('bono','dia'), condition=models.Q(fecha__isnull=True), name='bv_registro_legacy_dia_unique')),
    ]
