from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

import pandas as pd
from django.core.management.base import BaseCommand

from recetas.utils.normalizacion import normalizar_nombre


EPOCH = date(1899, 12, 30)


def excel_date(n):
    if not n or str(n) in ("nan", "0"):
        return None
    try:
        return EPOCH + timedelta(days=int(float(n)))
    except Exception:
        return None


def decimal_value(raw, default="0") -> Decimal:
    try:
        return Decimal(str(raw if raw not in (None, "") else default)).quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(str(default)).quantize(Decimal("0.01"))


class Command(BaseCommand):
    help = "Migra historial de préstamos desde Excel a la BD"

    def add_arguments(self, parser):
        parser.add_argument("--archivo", required=True)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--ejecutar", action="store_true")

    def handle(self, *args, **options):
        from rrhh.models import Empleado, Prestamo, PrestamoCuota

        if not options["ejecutar"] and not options["dry_run"]:
            self.stdout.write("Usa --dry-run o --ejecutar")
            return

        df = pd.read_excel(options["archivo"], sheet_name="PRESTAMOS PERSONAL", header=2)
        creados = 0
        errores = 0

        for _, row in df.iterrows():
            nombre = str(row.get("PERSONAL", "")).strip()
            if not nombre or nombre == "nan":
                continue

            total = row.get("TOTAL DE PRESTAMO")
            quincenas = row.get("NUMERO DE QUINCENAS A DESCONTAR")
            cobro_q = row.get("COBRO POR QUINCENA")
            f_ini = excel_date(row.get("FECHA INICIO COBRO"))
            saldo = row.get("SALDO ACTUAL", 0)

            if str(total) in ("nan", "0", "") or not f_ini:
                continue

            nombre_norm = normalizar_nombre(nombre)
            emp = None
            for candidato in Empleado.objects.filter(activo=True).only("id", "nombre", "nombre_normalizado"):
                empleado_norm = candidato.nombre_normalizado or normalizar_nombre(candidato.nombre)
                if set(nombre_norm.split()).issubset(set(empleado_norm.split())) or set(empleado_norm.split()).issubset(
                    set(nombre_norm.split())
                ):
                    emp = candidato
                    break

            if not emp:
                self.stdout.write(f"[ERROR] No encontrado: {nombre}")
                errores += 1
                continue

            total_dec = decimal_value(total)
            quincenas_int = int(float(quincenas or 1))
            cobro_q_dec = decimal_value(cobro_q)
            saldo_dec = decimal_value(saldo)

            if options["dry_run"]:
                self.stdout.write(f"[DRY] {nombre} -> ${total_dec} en {quincenas_int}Q desde {f_ini}")
                continue

            estado = Prestamo.ESTADO_LIQUIDADO if saldo_dec == Decimal("0.00") else Prestamo.ESTADO_ACTIVO
            prestamo = Prestamo.objects.create(
                empleado=emp,
                concepto="Migrado desde historial Excel",
                metodo_pago=Prestamo.METODO_TRANSFERENCIA,
                fecha_solicitud=f_ini,
                fecha_deposito=f_ini,
                importe=total_dec,
                num_quincenas=quincenas_int,
                descuento_quincenal=cobro_q_dec,
                saldo_actual=saldo_dec,
                estado=estado,
                firma_jefe=True,
                firma_direccion=True,
            )

            cols = list(df.columns)
            hist_start = cols.index("HISTORIAL DE DESCUENTO") if "HISTORIAL DE DESCUENTO" in cols else None
            if hist_start:
                hist_cols = cols[hist_start:]
                q_num = 1
                for i in range(0, len(hist_cols) - 1, 2):
                    f_col = hist_cols[i]
                    c_col = hist_cols[i + 1] if i + 1 < len(hist_cols) else None
                    f_val = excel_date(row.get(f_col))
                    c_val = row.get(c_col) if c_col else None
                    if f_val and c_val and str(c_val) not in ("nan", "0"):
                        PrestamoCuota.objects.create(
                            prestamo=prestamo,
                            numero_quincena=q_num,
                            fecha_quincena=f_val,
                            monto_esperado=prestamo.descuento_quincenal,
                            monto_cobrado=decimal_value(c_val),
                            estado=PrestamoCuota.ESTADO_COBRADO,
                            fuente=PrestamoCuota.FUENTE_MANUAL,
                            fecha_cobro=f_val,
                        )
                        q_num += 1

            creados += 1
            self.stdout.write(f"[OK] {nombre} -> {prestamo.folio}")

        self.stdout.write(f"\nResumen: {creados} préstamos creados, {errores} errores")
