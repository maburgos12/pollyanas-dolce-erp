from django.core.management.base import BaseCommand

from core.models import Sucursal
from logistica.models import Unidad


class Command(BaseCommand):
    help = "Carga unidades base de logística de forma idempotente."

    UNIDADES = [
        {
            "codigo": "GS-P1",
            "descripcion": "Peugeot Partner",
            "placa": "UL-71-534",
            "color": "Blanco",
            "modelo": "2022",
            "marca": "Peugeot",
            "activa": True,
        },
        {
            "codigo": "GS-DC1",
            "descripcion": "Fiat Ducato Cargo Van",
            "placa": "VSS-772-C",
            "color": "Blanco",
            "modelo": "2023",
            "marca": "Fiat",
            "activa": True,
        },
        {
            "codigo": "GS-PM1",
            "descripcion": "Peugeot Manager",
            "placa": "VMX-402-D",
            "color": "Blanco",
            "modelo": "2024",
            "marca": "Peugeot",
            "activa": True,
        },
        {
            "codigo": "GS-CH1",
            "descripcion": "Chevrolet Cheyenne",
            "placa": "UL-71-533",
            "color": "Plata",
            "modelo": "2018",
            "marca": "Chevrolet",
            "activa": True,
        },
    ]

    def handle(self, *args, **options):
        sucursal = Sucursal.objects.order_by("id").first()
        if not sucursal:
            sucursal, _ = Sucursal.objects.get_or_create(
                codigo="MATRIZ",
                defaults={"nombre": "MATRIZ", "activa": True},
            )

        creadas = 0
        existentes = 0
        for row in self.UNIDADES:
            _, created = Unidad.objects.get_or_create(
                codigo=row["codigo"],
                defaults={**row, "sucursal": sucursal},
            )
            if created:
                creadas += 1
            else:
                existentes += 1

        self.stdout.write(self.style.SUCCESS(f"Unidades cargadas: {creadas} creadas, {existentes} existentes"))
