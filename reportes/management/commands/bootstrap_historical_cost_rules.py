from __future__ import annotations

from django.core.management.base import BaseCommand

from maestros.models import Insumo
from reportes.models import ReglaCostoHistoricoInsumo


DEFAULT_ALIAS_RULES = [
    ("MAICENA", "Almidón", "Equivalencia operativa de fécula/almidón."),
    ("ETIQUETA CH", "EtiquetaChica", "Etiqueta chica equivalente para rebanadas y piezas chicas."),
    ("TROZOS CRUNCH", "Crunch Trozos", "Mismo insumo comercial con nombre alterno."),
    ("CHOCOLATE SEMI-AMARGO TURIN", "Chocolate Semiamargo", "Chocolate semiamargo equivalente en maestro."),
    ("BASE Y DOMO 15", "Domo Mini C15N", "Mismo código C15N en maestro legacy para minis."),
    ("Gragea circular colores", "Gragea Colores", "Decorado equivalente por nombre comercial."),
    ("CREMA BISCOFF LOTUS", "Crema Lotus", "Crema Lotus equivalente para recetas Biscoff/Lotus."),
    ("Nuez en Trozos", "Nuez Picada", "Corte equivalente de nuez en maestro histórico."),
    ("CUCHARA G", "Cuchara Fresas", "Cuchara grande usada en vasos/postres."),
    ("Vaso 12 Oz", "Vaso12oz", "Mismo vaso 12 oz en maestro legacy."),
    ("TAPA VASO 12 OZ", "Tapa Vasos Domo", "Tapa usada para vaso 12 oz en maestro legacy."),
    ("VASO 16 OZ", "Vaso16oz", "Vaso 16 oz equivalente con compra histórica en maestro legacy."),
    ("VASO 20 OZ", "Vaso20oz", "Vaso 20 oz equivalente con compra histórica en maestro legacy."),
    ("TAPA VASO 16-20 OZ", "Tapa Vasos Domo 16y20oz", "Tapa compartida 16-20 oz equivalente con compra histórica en maestro legacy."),
    ("Mono/Roscas", "Monito/Rosca", "Decorado equivalente para roscas cuando Point no devuelve costo de compra histórico directo."),
]

DEFAULT_NEXT_KNOWN_RULES = [
    ("SUSTITUTO DE CREMA", "Usar primer costo posterior disponible mientras entra histórico de compra."),
    ("Gragea Colores", "Usar primer costo posterior disponible para decorado cuando no hay compras previas."),
    ("MOLDE ALUMINIO CH AL-22", "Usar primer costo posterior disponible para cerrar histórico de tartas pay."),
    ("DB06 DOMO MINI CHEESECAKE", "Usar primer costo posterior real importado desde Point transfer history."),
    ("PAPEL ENCERADO LOGO", "Usar primer costo posterior real importado desde Point transfer history."),
    ("Galleta Oreo Base", "Usar primer costo posterior real importado desde Point transfer history."),
    ("CREMA AVELLANA", "Usar primer costo posterior real importado desde Point transfer history."),
    ("CAJA G", "Usar primer costo posterior disponible para venta directa del empaque cuando no hay compra del mismo mes."),
]


class Command(BaseCommand):
    help = "Carga reglas auditables de equivalencia y fallback para costo histórico mensual."

    def handle(self, *args, **options):
        created = 0
        updated = 0
        skipped = 0

        for source_name, target_name, notes in DEFAULT_ALIAS_RULES:
            source = Insumo.objects.filter(nombre__iexact=source_name).first()
            target = Insumo.objects.filter(nombre__iexact=target_name).first()
            if source is None or target is None:
                skipped += 1
                self.stdout.write(f"skip alias: {source_name} -> {target_name}")
                continue
            _rule, was_created = ReglaCostoHistoricoInsumo.objects.update_or_create(
                insumo_origen=source,
                metodo=ReglaCostoHistoricoInsumo.METODO_EQUIVALENCIA,
                insumo_referencia=target,
                defaults={
                    "activo": True,
                    "prioridad": 10,
                    "notas": notes,
                },
            )
            created += int(was_created)
            updated += int(not was_created)

        for source_name, notes in DEFAULT_NEXT_KNOWN_RULES:
            source = Insumo.objects.filter(nombre__iexact=source_name).first()
            if source is None:
                skipped += 1
                self.stdout.write(f"skip next_known: {source_name}")
                continue
            _rule, was_created = ReglaCostoHistoricoInsumo.objects.update_or_create(
                insumo_origen=source,
                metodo=ReglaCostoHistoricoInsumo.METODO_SIGUIENTE,
                insumo_referencia=None,
                defaults={
                    "activo": True,
                    "prioridad": 20,
                    "notas": notes,
                },
            )
            created += int(was_created)
            updated += int(not was_created)

        self.stdout.write(
            self.style.SUCCESS(
                f"bootstrap_historical_cost_rules created={created} updated={updated} skipped={skipped}"
            )
        )
