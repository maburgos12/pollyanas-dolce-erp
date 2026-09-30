"""Hace que el gasto de nómina por sucursal sea lo que de verdad cuesta.

Las reglas leían `salario_base`, que en ContPaQ es sólo el concepto «Sueldo»:
se verificó que la suma de ese campo empata al centavo con el concepto 1 de la
nómina. Todo lo demás que cobra la gente —aguinaldo, prima dominical, prima
vacacional, premios de puntualidad y asistencia, horas extra, indemnización—
quedaba fuera del gasto de la sucursal.

`total_percepciones` sí los incluye. Lo único que no incluye es la despensa:
la identidad `total_percepciones = conceptos de percepción − despensa` se
comprueba al centavo, y por eso la despensa sigue entrando por su propia regla.

Las reglas de los conceptos 19 y 20 se desactivan porque esos conceptos ya
están dentro de `total_percepciones`: dejarlas los contaría dos veces.

El costo patronal se completa con las cuotas del IMSS, que ya entran por SIPARE
y no se tocan aquí.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db import transaction

from reportes.models import LineaPresupuestoMensual, ReglaFuenteRubro

CAMPO_VIEJO = "salario_base"
CAMPO_NUEVO = "total_percepciones"

# Conceptos que `total_percepciones` ya trae dentro: sumarlos aparte duplica.
CONCEPTOS_YA_INCLUIDOS = {"19", "20"}
# La despensa queda fuera del campo, así que su regla se conserva.
CONCEPTO_DESPENSA = "32"


class Command(BaseCommand):
    help = "Cambia el gasto de nómina de salario_base a percepciones completas."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        with transaction.atomic():
            self._ampliar_campo()
            self._quitar_duplicados()
            self._abrir_renglones()
            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))

    def _ampliar_campo(self) -> None:
        self.stdout.write("REGLAS DE NÓMINA · de sueldo a percepciones completas")
        cambiadas = 0
        for regla in ReglaFuenteRubro.objects.filter(
            tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA, activa=True
        ).select_related("rubro", "rubro__sucursal", "rubro__area").order_by("id"):
            filtros = dict(regla.filtros or {})
            if filtros.get("campo_monto", CAMPO_VIEJO) != CAMPO_VIEJO:
                continue
            filtros["campo_monto"] = CAMPO_NUEVO
            regla.filtros = filtros
            regla.save(update_fields=["filtros", "actualizado_en"])
            destino = regla.rubro.sucursal or regla.rubro.area
            self.stdout.write(f"  regla {regla.id:<6} {str(destino)[:34]:<36} {regla.modo_asignacion}")
            cambiadas += 1
        self.stdout.write(f"  {cambiadas} reglas ahora leen percepciones completas")

    def _abrir_renglones(self) -> None:
        """Un rubro sin renglones no recibe nada, por buena que sea su regla.

        La consolidación escribe sobre líneas que ya existen; no las crea. Los
        rubros de despensa se dieron de alta con su regla pero sin renglones, así
        que su gasto nunca aterrizó. Se abren en cero: la despensa no se
        presupuestó aparte porque venía dentro de nómina, y el presupuesto no se
        inventa.
        """
        self.stdout.write("")
        self.stdout.write("RENGLONES QUE FALTABAN")
        rubros = {
            regla.rubro
            for regla in ReglaFuenteRubro.objects.filter(
                tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA_CONCEPTO, activa=True
            ).select_related("rubro")
        }
        abiertos = 0
        for rubro in sorted(rubros, key=lambda r: r.pk):
            existentes = set(
                LineaPresupuestoMensual.objects.filter(
                    rubro=rubro, periodo__year=2026
                ).values_list("periodo", flat=True)
            )
            for mes in range(1, 13):
                periodo = date(2026, mes, 1)
                if periodo in existentes:
                    continue
                LineaPresupuestoMensual.objects.create(
                    rubro=rubro,
                    periodo=periodo,
                    version=LineaPresupuestoMensual.VERSION_ORIGINAL,
                    monto_presupuesto=Decimal("0"),
                )
                abiertos += 1
        self.stdout.write(f"  {abiertos} renglones abiertos en {len(rubros)} rubros")

    def _quitar_duplicados(self) -> None:
        self.stdout.write("")
        self.stdout.write("CONCEPTOS QUE YA VIENEN DENTRO · se desactivan para no duplicar")
        desactivadas = 0
        conservadas = 0
        huerfanos = []
        for regla in ReglaFuenteRubro.objects.filter(
            tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA_CONCEPTO, activa=True
        ).select_related("rubro", "rubro__sucursal", "rubro__area").order_by("id"):
            codigos = {str(c).strip() for c in (regla.filtros or {}).get("codigos_concepto", [])}
            if codigos & {CONCEPTO_DESPENSA}:
                conservadas += 1
                continue
            if not codigos or not codigos <= CONCEPTOS_YA_INCLUIDOS:
                conservadas += 1
                continue
            regla.activa = False
            regla.notas = (
                (regla.notas or "")
                + " · Desactivada: el concepto ya viene dentro de total_percepciones."
            ).strip()
            regla.save(update_fields=["activa", "notas", "actualizado_en"])
            huerfanos.append(regla.rubro)
            desactivadas += 1
        self.stdout.write(f"  {desactivadas} reglas de los conceptos 19 y 20 desactivadas")
        self.stdout.write(f"  {conservadas} conservadas, entre ellas las de despensa")
        self.stdout.write(f"  {self._limpiar_huerfanos()} importes viejos borrados")

    @staticmethod
    def _limpiar_huerfanos() -> int:
        """Al quitarle la regla a un rubro, su importe se queda congelado.

        La consolidación protege el valor cuando la fuente viene vacía, así que
        el renglón seguiría mostrando el último número bueno para siempre. Se
        borra sólo lo que venía de esta misma fuente en rubros que ya no la
        tienen: el legado del Excel y las capturas manuales no se tocan, y por
        eso el barrido no es general.
        """
        con_regla = ReglaFuenteRubro.objects.filter(
            tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA_CONCEPTO, activa=True
        ).values_list("rubro_id", flat=True)
        return (
            LineaPresupuestoMensual.objects.filter(
                periodo__year=2026, fuente_real="AUTO:NOMINA_CONCEPTO"
            )
            .exclude(rubro_id__in=con_regla)
            .update(monto_real=None, fuente_real="")
        )
