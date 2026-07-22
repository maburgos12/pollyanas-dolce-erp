"""Aplica las reglas de clasificacion bancaria a los movimientos de un periodo.

Dry-run por defecto (reporta que haria); con --aplicar escribe tipo_conciliacion
en los movimientos SIN clasificar y NO conciliados. Nunca pisa clasificaciones
existentes ni toca el flag conciliado — eso queda para revision humana.

Uso:
  python manage.py clasificar_movimientos --periodo 2026-01            # dry-run
  python manage.py clasificar_movimientos --periodo 2026-01 --aplicar
"""
from __future__ import annotations

from datetime import datetime, timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from conciliacion.services.reglas_contables import propuestas_para_movimiento
from syncfy_client.models import MovimientoBancario

# concepto.codigo -> tipo_conciliacion del movimiento; el resto se mapea por familia
TIPO_POR_CODIGO = {
    "TRASPASO_ENTRE_CUENTAS": MovimientoBancario.CONCILIACION_TRASPASO,
    "DISPOSICION_LINEA_CREDITO": MovimientoBancario.CONCILIACION_LINEA_CREDITO,
    "PAGO_LINEA_CREDITO": MovimientoBancario.CONCILIACION_LINEA_CREDITO,
    "PRESTAMO_RECIBIDO": MovimientoBancario.CONCILIACION_LINEA_CREDITO,
    "PRESTAMO_PAGADO": MovimientoBancario.CONCILIACION_LINEA_CREDITO,
    "PAGO_TARJETA_CREDITO": MovimientoBancario.CONCILIACION_TARJETA_CREDITO,
    "COMISION_TPV": MovimientoBancario.CONCILIACION_COMISION,
    "IVA_COMISION_TPV": MovimientoBancario.CONCILIACION_COMISION,
}
TIPO_POR_FAMILIA = {
    "venta": MovimientoBancario.CONCILIACION_INGRESO_FACTURADO,
    "tarjeta": MovimientoBancario.CONCILIACION_INGRESO_FACTURADO,
    "transferencia": MovimientoBancario.CONCILIACION_CFDI,
    "gasto": MovimientoBancario.CONCILIACION_CFDI,
    "nomina": MovimientoBancario.CONCILIACION_NOMINA,
    "fiscal": MovimientoBancario.CONCILIACION_FISCAL,
    "balance": MovimientoBancario.CONCILIACION_REVISION,
    "pendiente": MovimientoBancario.CONCILIACION_REVISION,
}


def _tipo_para_concepto(concepto) -> str:
    return TIPO_POR_CODIGO.get(concepto.codigo) or TIPO_POR_FAMILIA.get(
        concepto.familia, MovimientoBancario.CONCILIACION_REVISION
    )


class Command(BaseCommand):
    help = "Clasifica movimientos bancarios de un periodo aplicando las reglas activas"

    def add_arguments(self, parser):
        parser.add_argument("--periodo", required=True, help="Periodo YYYY-MM")
        parser.add_argument("--aplicar", action="store_true", help="Escribir cambios (default: dry-run)")
        parser.add_argument("--cuenta", type=int, help="Limitar a una cuenta bancaria (id)")

    def handle(self, *args, **options):
        try:
            year, month = (int(x) for x in options["periodo"].split("-"))
            inicio = timezone.make_aware(datetime(year, month, 1))
        except (ValueError, TypeError) as exc:
            raise CommandError("Periodo invalido: usar YYYY-MM") from exc
        fin = timezone.make_aware(
            datetime(year + (month == 12), (month % 12) + 1, 1)
        )

        qs = MovimientoBancario.objects.filter(
            fecha_transaccion__gte=inicio,
            fecha_transaccion__lt=fin,
            conciliado=False,
            tipo_conciliacion="",
        ).select_related("cuenta").order_by("fecha_transaccion")
        if options.get("cuenta"):
            qs = qs.filter(cuenta_id=options["cuenta"])

        total = qs.count()
        por_regla: dict[str, int] = {}
        sin_regla = 0
        aplicados = 0
        for mov in qs.iterator(chunk_size=500):
            propuestas = propuestas_para_movimiento(mov)
            if not propuestas:
                sin_regla += 1
                continue
            mejor = max(propuestas, key=lambda p: (p.confianza, -p.regla.prioridad))
            etiqueta = f"{mejor.regla.nombre} -> {mejor.regla.concepto.codigo}"
            por_regla[etiqueta] = por_regla.get(etiqueta, 0) + 1
            if options["aplicar"]:
                mov.tipo_conciliacion = _tipo_para_concepto(mejor.regla.concepto)
                extra = dict(mov.extra_raw or {})
                extra["clasificacion_auto"] = {
                    "regla": mejor.regla.nombre,
                    "concepto": mejor.regla.concepto.codigo,
                    "confianza": mejor.confianza,
                    "fecha": timezone.now().isoformat(),
                }
                mov.extra_raw = extra
                mov.save(update_fields=["tipo_conciliacion", "extra_raw"])
                aplicados += 1

        modo = "APLICADO" if options["aplicar"] else "DRY-RUN"
        self.stdout.write(f"[{modo}] periodo {options['periodo']}: {total} movimientos sin clasificar")
        for etiqueta, n in sorted(por_regla.items(), key=lambda kv: -kv[1]):
            self.stdout.write(f"  {n:>5}  {etiqueta}")
        self.stdout.write(f"  {sin_regla:>5}  SIN REGLA (quedan en revision manual)")
        if options["aplicar"]:
            self.stdout.write(self.style.SUCCESS(f"{aplicados} movimientos clasificados"))
