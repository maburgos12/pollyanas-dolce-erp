import django.db.models.deletion
import django.utils.timezone
from django.conf import settings
from django.db import migrations, models
from django.db.models import Count


def crear_intentos_historicos(apps, schema_editor):
    Intento = apps.get_model("compras", "IntentoCompraDepartamental")
    Linea = apps.get_model("compras", "LineaOrdenCompraDepartamental")
    Compra = apps.get_model("compras", "CompraRealizadaDepartamental")
    Compromiso = apps.get_model("compras", "CompromisoCompraDepartamental")
    db = schema_editor.connection.alias

    lineas_item = set(Linea.objects.using(db).values_list("item_id", flat=True))
    compra_huerfana = Compra.objects.using(db).exclude(item_id__in=lineas_item).order_by("pk").first()
    if compra_huerfana is not None:
        raise RuntimeError(
            "No se puede migrar la compra departamental "
            f"{compra_huerfana.pk}: su artículo {compra_huerfana.item_id} no tiene línea de orden."
        )

    versiones = {}
    for linea in Linea.objects.using(db).select_related("item", "orden").order_by("item_id", "pk"):
        versiones[linea.item_id] = versiones.get(linea.item_id, 0) + 1
        estado = (
            "ENTREGADO"
            if linea.item.estado in ("PENDIENTE_CONFIRMACION", "RECIBIDO_CONFORME")
            else "VIGENTE"
        )
        intento = Intento.objects.using(db).create(
            item_id=linea.item_id,
            cotizacion_id=linea.cotizacion_id,
            numero=versiones[linea.item_id],
            estado=estado,
            creado_en=linea.orden.creado_en,
        )
        # auto_now usa el reloj de la migración; fijarlo al origen permite una reversa verificable.
        Intento.objects.using(db).filter(pk=intento.pk).update(actualizado_en=linea.orden.creado_en)
        Linea.objects.using(db).filter(pk=linea.pk).update(intento_id=intento.pk)
        Compra.objects.using(db).filter(item_id=linea.item_id).update(intento_id=intento.pk)
        Compromiso.objects.using(db).filter(item_id=linea.item_id).update(intento_id=intento.pk)


def comprobar_reversa_segura(apps, schema_editor):
    """Revertir solo datos que 0015 permitiría reconstruir sin pérdida."""
    db = schema_editor.connection.alias
    Intento = apps.get_model("compras", "IntentoCompraDepartamental")
    Reembolso = apps.get_model("compras", "ReembolsoCompraDepartamental")
    Linea = apps.get_model("compras", "LineaOrdenCompraDepartamental")
    Compra = apps.get_model("compras", "CompraRealizadaDepartamental")
    Compromiso = apps.get_model("compras", "CompromisoCompraDepartamental")
    if Reembolso.objects.using(db).exists():
        raise RuntimeError("No se puede volver a compras 0015: existen reembolsos que se perderían.")
    if Intento.objects.using(db).filter(linea_orden__isnull=True).exists():
        raise RuntimeError("No se puede volver a compras 0015: existen intentos sin línea de orden.")
    if Intento.objects.using(db).exclude(estado__in=("VIGENTE", "ENTREGADO")).exists():
        raise RuntimeError("No se puede volver a compras 0015: existen estados de intento no representables.")
    sin_cambios = models.Q(
        motivo_cancelacion="", detalle_cancelacion="", cancelado_en__isnull=True,
        cancelado_por__isnull=True, reembolso_solicitado__isnull=True,
        reembolso_solicitado_en__isnull=True, version=1,
    ) & (models.Q(evidencia_solicitud_reembolso="") | models.Q(evidencia_solicitud_reembolso__isnull=True))
    if Intento.objects.using(db).exclude(sin_cambios).exists():
        raise RuntimeError("No se puede volver a compras 0015: existen datos nuevos de cancelación o reembolso.")

    intentos = {intento.pk: intento for intento in Intento.objects.using(db).all()}
    esperados_por_item = {}
    numeros_por_item = {}
    for linea in Linea.objects.using(db).select_related("item", "orden").order_by("item_id", "pk"):
        intento = intentos.pop(linea.intento_id, None)
        if intento is None:
            raise RuntimeError(f"No se puede volver a compras 0015: la línea {linea.pk} no tiene intento único.")
        numeros_por_item[linea.item_id] = numeros_por_item.get(linea.item_id, 0) + 1
        numero_esperado = numeros_por_item[linea.item_id]
        estado_esperado = (
            "ENTREGADO"
            if linea.item.estado in ("PENDIENTE_CONFIRMACION", "RECIBIDO_CONFORME")
            else "VIGENTE"
        )
        if intento.item_id != linea.item_id or intento.cotizacion_id != linea.cotizacion_id:
            raise RuntimeError(f"No se puede volver a compras 0015: el intento {intento.pk} no coincide con su línea.")
        if intento.estado != estado_esperado:
            raise RuntimeError(f"No se puede volver a compras 0015: el estado del intento {intento.pk} no es reconstruible.")
        if intento.numero != numero_esperado:
            raise RuntimeError(f"No se puede volver a compras 0015: el número del intento {intento.pk} no es reconstruible.")
        if intento.creado_en != linea.orden.creado_en or intento.actualizado_en != linea.orden.creado_en:
            raise RuntimeError(f"No se puede volver a compras 0015: las fechas del intento {intento.pk} no son reconstruibles.")
        esperados_por_item[linea.item_id] = intento.pk
    if intentos:
        raise RuntimeError("No se puede volver a compras 0015: existen intentos sin línea reconstruible.")
    for compra in Compra.objects.using(db).all():
        if compra.intento_id != esperados_por_item.get(compra.item_id):
            raise RuntimeError(f"No se puede volver a compras 0015: la compra {compra.pk} apunta a otro intento.")
    for compromiso in Compromiso.objects.using(db).all():
        if compromiso.intento_id != esperados_por_item.get(compromiso.item_id):
            raise RuntimeError(f"No se puede volver a compras 0015: el compromiso {compromiso.pk} apunta a otro intento.")
    for nombre in ("LineaOrdenCompraDepartamental", "CompraRealizadaDepartamental", "CompromisoCompraDepartamental"):
        Modelo = apps.get_model("compras", nombre)
        repetido = (
            Modelo.objects.using(db)
            .values("item_id")
            .annotate(total=Count("pk"))
            .filter(total__gt=1)
            .order_by("item_id")
            .first()
        )
        if repetido:
            raise RuntimeError(
                f"No se puede volver a compras 0015: {nombre} tiene "
                f"{repetido['total']} registros para el artículo {repetido['item_id']}."
            )


class Migration(migrations.Migration):
    dependencies = [
        ("compras", "0015_comprarealizadadepartamental_version_and_more"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="IntentoCompraDepartamental",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("numero", models.PositiveIntegerField(editable=False)),
                ("version", models.PositiveIntegerField(default=1)),
                ("estado", models.CharField(
                    choices=[
                        ("VIGENTE", "Vigente"), ("CANCELADO_SIN_PAGO", "Cancelado sin pago"),
                        ("REEMBOLSO_SOLICITADO", "Reembolso solicitado"),
                        ("REEMBOLSADO", "Reembolsado"), ("ENTREGADO", "Entregado"),
                    ], db_index=True, default="VIGENTE", max_length=30,
                )),
                ("motivo_cancelacion", models.CharField(
                    blank=True, choices=[
                        ("PROVEEDOR_CANCELO", "Proveedor canceló"),
                        ("NO_ENTREGO", "Proveedor no entregó"), ("OTRO", "Otro"),
                    ], default="", max_length=30,
                )),
                ("detalle_cancelacion", models.TextField(blank=True, default="")),
                ("cancelado_en", models.DateTimeField(blank=True, null=True)),
                ("reembolso_solicitado", models.DecimalField(blank=True, decimal_places=2, max_digits=14, null=True)),
                ("reembolso_solicitado_en", models.DateField(blank=True, null=True)),
                ("evidencia_solicitud_reembolso", models.FileField(
                    blank=True, null=True, upload_to="compras/departamentales/reembolsos/solicitudes/%Y/%m/",
                )),
                ("creado_en", models.DateTimeField(default=django.utils.timezone.now)),
                ("actualizado_en", models.DateTimeField(auto_now=True)),
                ("cancelado_por", models.ForeignKey(
                    blank=True, null=True, on_delete=django.db.models.deletion.PROTECT,
                    related_name="intentos_compra_cancelados", to=settings.AUTH_USER_MODEL,
                )),
                ("cotizacion", models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name="intentos_compra", to="compras.cotizacioncompradepartamental",
                )),
                ("item", models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name="intentos_compra", to="compras.itemcompradepartamental",
                )),
            ],
            options={"ordering": ["numero", "pk"]},
        ),
        migrations.CreateModel(
            name="ReembolsoCompraDepartamental",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("importe", models.DecimalField(decimal_places=2, max_digits=14)),
                ("fecha", models.DateField()),
                ("referencia", models.CharField(blank=True, default="", max_length=160)),
                ("comprobante", models.FileField(
                    blank=True, null=True, upload_to="compras/departamentales/reembolsos/recibidos/%Y/%m/",
                )),
                ("creado_en", models.DateTimeField(default=django.utils.timezone.now)),
                ("intento", models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name="reembolsos", to="compras.intentocompradepartamental",
                )),
                ("registrado_por", models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL,
                )),
            ],
            options={"ordering": ["creado_en", "pk"]},
        ),
        migrations.AddField(
            model_name="lineaordencompradepartamental", name="intento",
            field=models.OneToOneField(
                blank=True, null=True, on_delete=django.db.models.deletion.PROTECT,
                related_name="linea_orden", to="compras.intentocompradepartamental",
            ),
        ),
        migrations.AddField(
            model_name="comprarealizadadepartamental", name="intento",
            field=models.OneToOneField(
                blank=True, null=True, on_delete=django.db.models.deletion.PROTECT,
                related_name="compra", to="compras.intentocompradepartamental",
            ),
        ),
        migrations.AddField(
            model_name="compromisocompradepartamental", name="intento",
            field=models.OneToOneField(
                blank=True, null=True, on_delete=django.db.models.deletion.PROTECT,
                related_name="compromiso", to="compras.intentocompradepartamental",
            ),
        ),
        migrations.AlterField(
            model_name="lineaordencompradepartamental", name="item",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT, related_name="lineas_orden",
                to="compras.itemcompradepartamental",
            ),
        ),
        migrations.AlterField(
            model_name="comprarealizadadepartamental", name="item",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT, related_name="compras_realizadas",
                to="compras.itemcompradepartamental",
            ),
        ),
        migrations.AlterField(
            model_name="compromisocompradepartamental", name="item",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE, related_name="compromisos",
                to="compras.itemcompradepartamental",
            ),
        ),
        migrations.RunPython(crear_intentos_historicos, comprobar_reversa_segura),
        migrations.AlterField(
            model_name="lineaordencompradepartamental", name="intento",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.PROTECT, related_name="linea_orden",
                to="compras.intentocompradepartamental",
            ),
        ),
        migrations.AlterField(
            model_name="comprarealizadadepartamental", name="intento",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.PROTECT, related_name="compra",
                to="compras.intentocompradepartamental",
            ),
        ),
        migrations.AddConstraint(
            model_name="intentocompradepartamental",
            constraint=models.UniqueConstraint(
                condition=models.Q(estado="VIGENTE"), fields=("item",), name="comp_dept_un_intento_vigente",
            ),
        ),
        migrations.AddConstraint(
            model_name="intentocompradepartamental",
            constraint=models.UniqueConstraint(fields=("item", "numero"), name="comp_dept_intento_numero_unico"),
        ),
        migrations.AddConstraint(
            model_name="intentocompradepartamental",
            constraint=models.CheckConstraint(check=models.Q(numero__gt=0), name="comp_dept_intento_numero_positivo"),
        ),
        migrations.AddConstraint(
            model_name="intentocompradepartamental",
            constraint=models.CheckConstraint(
                check=models.Q(reembolso_solicitado__isnull=True) | models.Q(reembolso_solicitado__gt=0),
                name="comp_dept_solicitud_reembolso_positiva",
            ),
        ),
        migrations.AddConstraint(
            model_name="reembolsocompradepartamental",
            constraint=models.CheckConstraint(check=models.Q(importe__gt=0), name="comp_dept_reembolso_positivo"),
        ),
        migrations.AddConstraint(
            model_name="compromisocompradepartamental",
            constraint=models.UniqueConstraint(
                condition=models.Q(activo=True, intento__isnull=True), fields=("item",),
                name="comp_dept_una_reserva_preorden_activa",
            ),
        ),
    ]
