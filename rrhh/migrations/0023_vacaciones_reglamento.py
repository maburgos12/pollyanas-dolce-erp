from datetime import date
from decimal import Decimal

import django.db.models.deletion
import django.utils.timezone
from django.conf import settings
from django.db import migrations, models


def cargar_reglamento_vacaciones(apps, schema_editor):
    ReglamentoLaboral = apps.get_model("rrhh", "ReglamentoLaboral")
    ReglaLaboral = apps.get_model("rrhh", "ReglaLaboral")
    PoliticaVacaciones = apps.get_model("rrhh", "PoliticaVacaciones")

    reglamento, _created = ReglamentoLaboral.objects.get_or_create(
        nombre="Reglamento interno FONSMA",
        version="2026-04-09",
        defaults={
            "empresa": "GRUPO EMPRESARIAL FONSMA S.A. DE C.V.",
            "fecha_documento": date(2026, 4, 9),
            "estado": "vigente",
            "fuente_archivo": "REGLAMENTO INTERNO FONSMA.doc",
            "notas": "Carga inicial para prototipo de Capital Humano y vacaciones.",
        },
    )

    reglas = [
        (
            "art-18-faltas",
            "CAPITULO VI. DE LAS FALTAS",
            "ARTICULO 18",
            "permisos",
            "Solicitud anticipada de faltas",
            "La solicitud de autorización para faltas a las labores deberá ser hecha con tres días de anticipación al Patrón o a su Jefe inmediato. El solo hecho de avisar no implica concesión del permiso.",
            10,
        ),
        (
            "art-24-descanso",
            "CAPITULO IX. DESCANSO, LICENCIAS Y VACACIONES",
            "ARTICULO 24",
            "descanso",
            "Descanso semanal",
            "La Empresa concederá a sus trabajadores un día de descanso por cada seis días de trabajo.",
            20,
        ),
        (
            "art-25-descanso-obligatorio",
            "CAPITULO IX. DESCANSO, LICENCIAS Y VACACIONES",
            "ARTICULO 25",
            "descanso",
            "Días de descanso obligatorio",
            "Son días de descanso obligatorio con goce íntegro de salario los señalados por el artículo 74 de la Ley Federal del Trabajo.",
            30,
        ),
        (
            "art-27-paternidad",
            "CAPITULO IX. DESCANSO, LICENCIAS Y VACACIONES",
            "ARTICULO 27",
            "permisos",
            "Licencia de paternidad",
            "El trabajador próximo a ser papá tendrá derecho a un permiso con goce de sueldo de cinco días laborables por nacimiento o adopción.",
            40,
        ),
        (
            "art-28-lactancia",
            "CAPITULO IX. DESCANSO, LICENCIAS Y VACACIONES",
            "ARTICULO 28",
            "permisos",
            "Permiso de lactancia",
            "Las trabajadoras madres lactantes tendrán derecho a una hora de permiso al inicio o al final de su jornada laboral, previo acuerdo con su jefe inmediato y Recursos Humanos.",
            50,
        ),
        (
            "art-29-vacaciones-lft",
            "CAPITULO IX. DESCANSO, LICENCIAS Y VACACIONES",
            "ARTICULO 29",
            "vacaciones",
            "Vacaciones conforme a LFT",
            "La Empresa concederá vacaciones anuales conforme al artículo 76 de la Ley Federal del Trabajo, pagadas con salario íntegro.",
            60,
        ),
        (
            "art-30-dias-laborables",
            "CAPITULO IX. DESCANSO, LICENCIAS Y VACACIONES",
            "ARTICULO 30",
            "vacaciones",
            "Cómputo por días laborables",
            "Para el cómputo de vacaciones se incluirán únicamente los días laborables, no incluidos en descanso semanal ni en los días de descanso del artículo 74 de la Ley Federal del Trabajo.",
            70,
        ),
        (
            "art-31-no-acumulacion",
            "CAPITULO IX. DESCANSO, LICENCIAS Y VACACIONES",
            "ARTICULO 31",
            "vacaciones",
            "No acumulación de periodos",
            "No se podrán acumular vacaciones de un periodo a otro; el trabajador debe disfrutar todos los días que le correspondan hasta por lo menos un mes de anticipación al próximo periodo.",
            80,
        ),
    ]
    for clave, capitulo, articulo, tipo, titulo, texto, orden in reglas:
        ReglaLaboral.objects.get_or_create(
            reglamento=reglamento,
            clave=clave,
            defaults={
                "capitulo": capitulo,
                "articulo": articulo,
                "tipo": tipo,
                "titulo": titulo,
                "texto": texto,
                "orden": orden,
            },
        )

    politicas = [
        (1, 1, "12.00"),
        (2, 2, "14.00"),
        (3, 3, "16.00"),
        (4, 4, "18.00"),
        (5, 5, "20.00"),
        (6, 10, "22.00"),
        (11, 15, "24.00"),
        (16, 20, "26.00"),
        (21, 25, "28.00"),
        (26, 30, "30.00"),
        (31, None, "32.00"),
    ]
    for desde, hasta, dias in politicas:
        PoliticaVacaciones.objects.get_or_create(
            reglamento=reglamento,
            antiguedad_desde=desde,
            antiguedad_hasta=hasta,
            defaults={
                "nombre": "Vacaciones LFT vigente",
                "dias_laborables": Decimal(dias),
                "prima_porcentaje": Decimal("25.00"),
                "vigente_desde": date(2026, 4, 9),
                "activo": True,
                "notas": "Política base derivada del artículo 76 LFT referenciado por el reglamento interno.",
            },
        )


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("rrhh", "0022_catalogofuncionoperativa"),
    ]

    operations = [
        migrations.CreateModel(
            name="ReglamentoLaboral",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nombre", models.CharField(max_length=180)),
                ("empresa", models.CharField(default="GRUPO EMPRESARIAL FONSMA S.A. DE C.V.", max_length=180)),
                ("version", models.CharField(default="2026-04-09", max_length=40)),
                ("fecha_documento", models.DateField(blank=True, null=True)),
                ("estado", models.CharField(choices=[("borrador", "Borrador"), ("vigente", "Vigente"), ("archivado", "Archivado")], db_index=True, default="borrador", max_length=16)),
                ("fuente_archivo", models.CharField(blank=True, default="", max_length=240)),
                ("notas", models.TextField(blank=True, default="")),
                ("creado_en", models.DateTimeField(auto_now_add=True)),
                ("actualizado_en", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Reglamento laboral",
                "verbose_name_plural": "Reglamentos laborales",
                "ordering": ["-fecha_documento", "nombre"],
            },
        ),
        migrations.CreateModel(
            name="ReglaLaboral",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("clave", models.CharField(max_length=60)),
                ("capitulo", models.CharField(blank=True, default="", max_length=120)),
                ("articulo", models.CharField(blank=True, default="", max_length=40)),
                ("tipo", models.CharField(choices=[("vacaciones", "Vacaciones"), ("descanso", "Descanso"), ("permisos", "Permisos"), ("asistencia", "Asistencia"), ("disciplina", "Disciplina")], db_index=True, max_length=20)),
                ("titulo", models.CharField(max_length=160)),
                ("texto", models.TextField()),
                ("aplica_en_sistema", models.BooleanField(default=True)),
                ("orden", models.PositiveSmallIntegerField(default=0)),
                ("creado_en", models.DateTimeField(auto_now_add=True)),
                ("reglamento", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="reglas", to="rrhh.reglamentolaboral")),
            ],
            options={
                "verbose_name": "Regla laboral",
                "verbose_name_plural": "Reglas laborales",
                "ordering": ["orden", "id"],
                "unique_together": {("reglamento", "clave")},
            },
        ),
        migrations.CreateModel(
            name="PoliticaVacaciones",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nombre", models.CharField(default="Vacaciones LFT vigente", max_length=140)),
                ("antiguedad_desde", models.PositiveSmallIntegerField()),
                ("antiguedad_hasta", models.PositiveSmallIntegerField(blank=True, null=True)),
                ("dias_laborables", models.DecimalField(decimal_places=2, max_digits=5)),
                ("prima_porcentaje", models.DecimalField(decimal_places=2, default=Decimal("25.00"), max_digits=5)),
                ("vigente_desde", models.DateField(default=django.utils.timezone.localdate)),
                ("vigente_hasta", models.DateField(blank=True, null=True)),
                ("activo", models.BooleanField(db_index=True, default=True)),
                ("notas", models.TextField(blank=True, default="")),
                ("reglamento", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="politicas_vacaciones", to="rrhh.reglamentolaboral")),
            ],
            options={
                "verbose_name": "Política de vacaciones",
                "verbose_name_plural": "Políticas de vacaciones",
                "ordering": ["antiguedad_desde"],
            },
        ),
        migrations.CreateModel(
            name="SolicitudVacaciones",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("fecha_inicio", models.DateField()),
                ("fecha_fin", models.DateField()),
                ("dias_laborables", models.DecimalField(decimal_places=2, default=Decimal("0"), max_digits=5)),
                ("motivo", models.TextField(blank=True, default="")),
                ("estado", models.CharField(choices=[("solicitada", "Solicitada"), ("preautorizada", "Preautorizada por jefe"), ("aprobada", "Aprobada"), ("rechazada", "Rechazada"), ("cancelada", "Cancelada")], db_index=True, default="solicitada", max_length=20)),
                ("fecha_preautorizacion", models.DateTimeField(blank=True, null=True)),
                ("fecha_aprobacion_rrhh", models.DateTimeField(blank=True, null=True)),
                ("folio", models.CharField(editable=False, max_length=20, unique=True)),
                ("notas_rrhh", models.TextField(blank=True, default="")),
                ("creado_en", models.DateTimeField(auto_now_add=True)),
                ("actualizado_en", models.DateTimeField(auto_now=True)),
                ("aprobado_rrhh_por", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="vacaciones_aprobadas_rrhh", to=settings.AUTH_USER_MODEL)),
                ("creado_por", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="vacaciones_solicitadas", to=settings.AUTH_USER_MODEL)),
                ("empleado", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="solicitudes_vacaciones", to="rrhh.empleado")),
                ("jefe_directo", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="vacaciones_por_preautorizar", to=settings.AUTH_USER_MODEL)),
                ("preautorizado_por", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="vacaciones_preautorizadas", to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "verbose_name": "Solicitud de vacaciones",
                "verbose_name_plural": "Solicitudes de vacaciones",
                "ordering": ["-creado_en"],
            },
        ),
        migrations.CreateModel(
            name="MovimientoVacaciones",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tipo", models.CharField(choices=[("generado", "Generado"), ("reservado", "Reservado"), ("consumido", "Consumido"), ("liberado", "Liberado"), ("ajuste", "Ajuste manual")], db_index=True, max_length=16)),
                ("dias", models.DecimalField(decimal_places=2, max_digits=6)),
                ("periodo_anio", models.PositiveSmallIntegerField(db_index=True)),
                ("descripcion", models.CharField(blank=True, default="", max_length=220)),
                ("creado_en", models.DateTimeField(auto_now_add=True)),
                ("actor", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to=settings.AUTH_USER_MODEL)),
                ("empleado", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="movimientos_vacaciones", to="rrhh.empleado")),
                ("solicitud", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="movimientos", to="rrhh.solicitudvacaciones")),
            ],
            options={
                "verbose_name": "Movimiento de vacaciones",
                "verbose_name_plural": "Movimientos de vacaciones",
                "ordering": ["-creado_en", "-id"],
            },
        ),
        migrations.RunPython(cargar_reglamento_vacaciones, migrations.RunPython.noop),
    ]
