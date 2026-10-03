"""Abre los renglones que le faltan a un rubro con fuente activa.

La consolidación escribe sobre líneas que ya existen: nunca las crea. Un rubro
con una regla impecable y sin `LineaPresupuestoMensual` no recibe nada y se ve
igual que uno sin fuente. Pasó con Bamoa y Crucero —9 obligaciones de renta sin
dónde caer— y antes con ventas, nómina y los agregados de cadena, donde cada
comando terminó abriendo sus propios renglones a mano.

Este comando los abre en cero, que es lo honesto: el presupuesto del año se
autorizó sin ese rubro, así que no hay cifra autorizada que inventarle. Sólo
toca rubros activos y con al menos una regla activa; un rubro sin fuente seguiría
igual de vacío con renglones.

El rango de meses es explícito porque no todo rubro vive el año entero: una
sucursal que cerró en junio y la que la reemplazó en julio se reparten el año, y
abrirle doce meses a cada una inventa vida que no tuvo.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import LineaPresupuestoMensual, ReglaFuenteRubro, RubroPresupuesto


def _mes(texto: str) -> date:
    try:
        anio, mes = str(texto).split("-")
        return date(int(anio), int(mes), 1)
    except (ValueError, AttributeError) as exc:
        raise CommandError(f"Mes inválido '{texto}' (formato YYYY-MM)") from exc


def _meses(desde: date, hasta: date):
    actual = desde
    while actual <= hasta:
        yield actual
        actual = date(actual.year + (actual.month == 12), actual.month % 12 + 1, 1)


class Command(BaseCommand):
    help = "Abre en cero los renglones faltantes de los rubros que tienen fuente activa."

    def add_arguments(self, parser):
        parser.add_argument("--desde", required=True, help="Mes inicial YYYY-MM.")
        parser.add_argument("--hasta", required=True, help="Mes final YYYY-MM.")
        parser.add_argument("--rubro", nargs="*", type=int, help="Limita a estos ids de rubro.")
        parser.add_argument("--tipo-fuente", nargs="*", help="Limita a estos tipos de fuente.")
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")

    def handle(self, *args, **options):
        desde, hasta = _mes(options["desde"]), _mes(options["hasta"])
        if desde > hasta:
            raise CommandError("El mes inicial va después del final.")

        reglas = ReglaFuenteRubro.objects.filter(activa=True)
        if options.get("tipo_fuente"):
            reglas = reglas.filter(tipo_fuente__in=options["tipo_fuente"])

        rubros = RubroPresupuesto.objects.filter(
            activo=True, pk__in=reglas.values("rubro_id")
        ).select_related("area", "sucursal")
        if options.get("rubro"):
            rubros = rubros.filter(pk__in=options["rubro"])

        meses = list(_meses(desde, hasta))
        pendientes = []
        for rubro in rubros.order_by("area__orden", "concepto", "pk"):
            existentes = set(
                LineaPresupuestoMensual.objects.filter(
                    rubro=rubro,
                    periodo__gte=desde,
                    periodo__lte=hasta,
                    version=LineaPresupuestoMensual.VERSION_ORIGINAL,
                ).values_list("periodo", flat=True)
            )
            faltan = [periodo for periodo in meses if periodo not in existentes]
            if faltan:
                pendientes.append((rubro, faltan))

        self.stdout.write(
            f"RUBROS CON FUENTE Y SIN RENGLÓN · {desde:%b-%Y} a {hasta:%b-%Y}"
        )
        self.stdout.write("")
        if not pendientes:
            self.stdout.write("  ninguno: los rubros con fuente activa ya tienen sus renglones")
            return

        with transaction.atomic():
            total = 0
            for rubro, faltan in pendientes:
                for periodo in faltan:
                    LineaPresupuestoMensual.objects.create(
                        rubro=rubro,
                        periodo=periodo,
                        version=LineaPresupuestoMensual.VERSION_ORIGINAL,
                        monto_presupuesto=Decimal("0"),
                    )
                total += len(faltan)
                destino = rubro.sucursal or rubro.area
                self.stdout.write(
                    f"  {str(destino)[:30]:<32} {rubro.concepto[:30]:<32} "
                    f"{len(faltan):>2} renglones ({', '.join(f'{p:%Y-%m}' for p in faltan)})"
                )
            self.stdout.write("")
            self.stdout.write(f"  {len(pendientes)} rubros · {total} renglones en cero")
            if not options["apply"]:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(
            self.style.SUCCESS("[APLICADO]" if options["apply"] else "[SIMULACRO] nada se escribió")
        )
