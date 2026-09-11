"""Identidad técnica del activo y token QR permanente.

`qr_token` no puede añadirse en un solo `AddField` con `unique=True`: Django
evalúa el default una sola vez y escribiría el mismo UUID en todas las filas
existentes, chocando contra el índice único. Se añade nullable, se rellena fila
por fila y sólo entonces se impone la unicidad.
"""

import uuid

from django.db import migrations, models
import django.db.models.deletion


def poblar_qr_token(apps, schema_editor):
    Activo = apps.get_model("activos", "Activo")
    pendientes = Activo.objects.filter(qr_token__isnull=True).only("id")
    # ponytail: lotes de 500 bastan para el padrón real de activos (cientos);
    #           si algún día son cientos de miles, subir el tamaño del lote.
    lote = []
    for activo in pendientes.iterator(chunk_size=500):
        activo.qr_token = uuid.uuid4()
        lote.append(activo)
        if len(lote) >= 500:
            Activo.objects.bulk_update(lote, ["qr_token"])
            lote = []
    if lote:
        Activo.objects.bulk_update(lote, ["qr_token"])


class Migration(migrations.Migration):

    dependencies = [
        ("maestros", "0001_initial"),
        ("activos", "0005_trazabilidad_mantenimiento"),
    ]

    operations = [
        migrations.AddField(
            model_name="activo",
            name="qr_token",
            field=models.UUIDField(editable=False, null=True, verbose_name="Token QR"),
        ),
        migrations.RunPython(poblar_qr_token, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="activo",
            name="qr_token",
            field=models.UUIDField(
                default=uuid.uuid4,
                editable=False,
                help_text="Identificador permanente impreso en la etiqueta QR del activo",
                unique=True,
                verbose_name="Token QR",
            ),
        ),
        migrations.AddField(
            model_name="activo",
            name="marca",
            field=models.CharField(blank=True, default="", max_length=100),
        ),
        migrations.AddField(
            model_name="activo",
            name="modelo",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
        migrations.AddField(
            model_name="activo",
            name="numero_serie",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Sin unicidad: muchos equipos históricos no tienen placa legible",
                max_length=160,
                verbose_name="Número de serie",
            ),
        ),
        migrations.AddField(
            model_name="activo",
            name="proveedor_compra",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="activos_vendidos",
                to="maestros.proveedor",
                verbose_name="Proveedor de compra",
            ),
        ),
        migrations.AddField(
            model_name="activo",
            name="fecha_compra",
            field=models.DateField(blank=True, null=True, verbose_name="Fecha de compra"),
        ),
        migrations.AddField(
            model_name="activo",
            name="costo_adquisicion",
            field=models.DecimalField(
                blank=True, decimal_places=2, max_digits=18, null=True, verbose_name="Costo de adquisición"
            ),
        ),
        migrations.AddField(
            model_name="activo",
            name="garantia_hasta",
            field=models.DateField(blank=True, null=True, verbose_name="Garantía hasta"),
        ),
    ]
