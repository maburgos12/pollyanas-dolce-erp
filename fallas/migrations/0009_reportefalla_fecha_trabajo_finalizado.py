from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("fallas", "0008_reportefalla_duplicado_de_and_more")]
    operations = [migrations.AddField(
        model_name="reportefalla", name="fecha_trabajo_finalizado",
        field=models.DateField(null=True, blank=True, help_text="Fecha real informada del trabajo; no implica una hora ni un pago."),
    )]
