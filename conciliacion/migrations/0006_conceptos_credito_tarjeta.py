from django.db import migrations


CONCEPTOS = [
    {
        "codigo": "DISPOSICION_LINEA_CREDITO",
        "nombre": "Disposicion de linea de credito",
        "descripcion": "Entrada de recursos desde una linea de credito bancaria; no es venta.",
        "familia": "balance",
        "tipo_movimiento": "abono",
        "cfdi_esperado": "ninguno",
        "requiere_evidencia_externa": True,
        "afecta_flujo": True,
        "palabras_clave": ["disposicion", "linea de credito", "credito revolvente"],
        "evidencia_requerida": ["contrato", "tabla_amortizacion", "referencia_bancaria"],
        "prioridad": 84,
    },
    {
        "codigo": "PAGO_LINEA_CREDITO",
        "nombre": "Pago de linea de credito",
        "descripcion": "Salida bancaria para amortizar capital, intereses o comisiones de una linea de credito.",
        "familia": "balance",
        "tipo_movimiento": "cargo",
        "cfdi_esperado": "ninguno",
        "requiere_evidencia_externa": True,
        "afecta_flujo": True,
        "palabras_clave": ["pago credito", "linea de credito", "amortizacion"],
        "evidencia_requerida": ["contrato", "tabla_amortizacion", "estado_cuenta_credito"],
        "prioridad": 85,
    },
    {
        "codigo": "PAGO_TARJETA_CREDITO",
        "nombre": "Pago de tarjeta de credito",
        "descripcion": "Pago desde banco a tarjeta corporativa; no es gasto directo sin estado de cuenta y CFDI soporte.",
        "familia": "balance",
        "tipo_movimiento": "cargo",
        "cfdi_esperado": "opcional",
        "requiere_evidencia_externa": True,
        "afecta_flujo": True,
        "palabras_clave": ["pago tarjeta", "tarjeta credito"],
        "evidencia_requerida": ["estado_cuenta_tarjeta", "cfdi_soporte", "referencia_bancaria"],
        "prioridad": 86,
    },
]


def cargar_conceptos(apps, schema_editor):
    concepto_model = apps.get_model("conciliacion", "ConceptoConciliacion")
    for item in CONCEPTOS:
        defaults = {key: value for key, value in item.items() if key != "codigo"}
        concepto_model.objects.update_or_create(codigo=item["codigo"], defaults=defaults)


def borrar_conceptos(apps, schema_editor):
    concepto_model = apps.get_model("conciliacion", "ConceptoConciliacion")
    concepto_model.objects.filter(codigo__in=[item["codigo"] for item in CONCEPTOS]).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("conciliacion", "0005_catalogos_contables_reglas"),
    ]

    operations = [
        migrations.RunPython(cargar_conceptos, borrar_conceptos),
    ]
