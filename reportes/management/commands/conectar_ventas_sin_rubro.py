"""Conecta al Estado de Resultados la venta que ningún rubro recogía.

Las reglas de venta listan producto por producto —79 de 91 funcionan así— y
cuando Point da de alta algo nuevo nadie lo agrega a ninguna lista. El efecto es
silencioso: el producto se vende, el dinero entra, y el reporte no lo ve.

En septiembre de 2026 eran 16 productos por $60,624 con IVA, que es exactamente
la diferencia entre lo que el reporte mostraba y la venta de su propia fuente.

Los pasteles, vasos, galletas y bollos van a su rubro propio, como el resto del
catálogo. Los letreros, cake toppers y velas no son alimento —son mercancía que
se revende y sí causa IVA— así que van juntos a un renglón aparte: mezclarlos con
la pastelería confundiría dos márgenes distintos.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import (
    AreaPresupuesto,
    LineaPresupuestoMensual,
    ReglaFuenteRubro,
    RubroPresupuesto,
)

# Las 91 reglas de venta leen el importe con IVA; un campo distinto aquí
# descuadraría el área contra su propia fuente.
CAMPO = "total_venta"

# Un rubro por producto, como todo el catálogo de ventas.
PROPIOS = [
    "Pastel Snickers Mini",
    "Pastel Zanahoria Mini",
    "Vaso Dot Cake Vainilla",
    "Vaso Dot Cake Chocolate",
    "Surtido Galletas Mini",
    "Paq. Galleta Chocolate Mini",
    "Decoración Bollo Patrio",
]

# Mercancía de reventa: no es alimento y causa IVA, a diferencia del postre.
MERCANCIA_CONCEPTO = "Mercancía y accesorios"
MERCANCIA = [
    "LETRERO HAPPY BIRTHDAY PLATA",
    "LETRERO FELIZ CUMPLEAÑOS PLATA",
    "LETRERO FELIZ CUMPLEAÑOS NEGRO",
    "LETRERO HAPPY BIRTHDAY NEGRO",
    "CAKE TOPPER FELIZ CUMPLE ESTRELLA NEGRO",
    "CAKE TOPPER MAKE A WISH ROSA",
    "CAKE TOPPER FELIZ CUMPLE ESTRELLA PLATA",
    "VELA HUMO AZUL",
    "VELA HUMO ROSA",
]


class Command(BaseCommand):
    help = "Da de alta los rubros de venta que faltaban y los conecta a Point."

    def add_arguments(self, parser):
        parser.add_argument("--anio", type=int, default=2026)
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        anio = options["anio"]
        area = AreaPresupuesto.objects.filter(codigo="ventas").first()
        if area is None:
            raise CommandError("No existe el área de presupuesto 'ventas'.")

        with transaction.atomic():
            self.stdout.write("RUBROS PROPIOS · uno por producto, como el resto del catálogo")
            for producto in PROPIOS:
                self._conectar(area, producto, [producto], anio)

            self.stdout.write("")
            self.stdout.write("MERCANCÍA · no es alimento y causa IVA")
            self._conectar(area, MERCANCIA_CONCEPTO, MERCANCIA, anio)

            self.stdout.write("")
            self.stdout.write(
                f"  {len(PROPIOS) + 1} rubros conectados · "
                f"{len(PROPIOS) + len(MERCANCIA)} productos de Point"
            )
            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))

    def _conectar(self, area, concepto, productos, anio) -> None:
        rubro, creado = RubroPresupuesto.objects.get_or_create(
            concepto=concepto,
            area=area,
            sucursal=None,
            defaults={"tipo": RubroPresupuesto.TIPO_INGRESO, "activo": True},
        )
        if not creado and not rubro.activo:
            rubro.activo = True
            rubro.save(update_fields=["activo", "actualizado_en"])

        regla, regla_nueva = ReglaFuenteRubro.objects.get_or_create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_VENTA_POS,
            defaults={
                "activa": True,
                "filtros": {"campo_monto": CAMPO, "productos_pos": productos},
                "notas": "Producto que Point vendía y ninguna regla recogía.",
            },
        )
        if not regla_nueva:
            filtros = dict(regla.filtros or {})
            filtros["campo_monto"] = CAMPO
            filtros["productos_pos"] = productos
            regla.filtros = filtros
            regla.activa = True
            regla.save(update_fields=["filtros", "activa", "actualizado_en"])

        # Un rubro sin renglones no recibe nada, por buena que sea su regla: la
        # consolidación escribe sobre líneas que existen. Se abren en cero porque
        # el presupuesto 2026 se autorizó sin estos productos.
        abiertos = 0
        for mes in range(1, 13):
            _linea, nueva = LineaPresupuestoMensual.objects.get_or_create(
                rubro=rubro,
                periodo=date(anio, mes, 1),
                version=LineaPresupuestoMensual.VERSION_ORIGINAL,
                defaults={"monto_presupuesto": Decimal("0")},
            )
            abiertos += int(nueva)
        marca = "alta" if creado else "reactivado" if not rubro.activo else "ya existía"
        detalle = f"{len(productos)} productos" if len(productos) > 1 else ""
        self.stdout.write(
            f"  {marca:<11} {concepto[:36]:<38} {detalle:<14} {abiertos:>2} renglones"
        )
