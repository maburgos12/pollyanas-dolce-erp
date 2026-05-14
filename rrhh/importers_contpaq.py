from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd

from recetas.utils.normalizacion import normalizar_nombre

from .models import Empleado, ImportacionNominaContpaq, Prestamo, PrestamoCuota
from .services_prestamos import aplicar_cobro_manual


def _parse_xls(archivo) -> list[dict]:
    """
    Parsea Lista de Raya CONTPAQ y extrae empleados con concepto 64.
    """
    df = pd.read_excel(archivo, sheet_name="Lista de raya", header=None, engine="xlrd")

    resultados = []
    empleado_actual = None

    for _, row in df.iterrows():
        c0 = str(row[0]).strip()
        c1 = str(row[1]).strip() if pd.notna(row[1]) else ""
        c5 = str(row[5]).strip() if pd.notna(row[5]) else ""
        c6 = str(row[6]).strip() if pd.notna(row[6]) else ""
        c8 = row[8]
        c6_norm = normalizar_nombre(c6)

        if (
            c0.isdigit()
            and c1 == c1.upper()
            and len(c1) > 5
            and "nan" not in c1.lower()
            and "." not in c0
            and c1 not in ("PRODUCCION", "VENTAS", "ADMINISTRACION", "LOGISTICA", "COMPRAS")
        ):
            if empleado_actual and empleado_actual.get("prestamo_empresa"):
                resultados.append(empleado_actual)
            empleado_actual = {
                "num_empleado": c0,
                "nombre": c1,
                "prestamo_empresa": None,
            }

        if empleado_actual and c5 == "64" and "prestamo empresa" in c6_norm:
            try:
                monto = Decimal(str(c8)).quantize(Decimal("0.01"))
                empleado_actual["prestamo_empresa"] = monto
            except Exception:
                pass

    if empleado_actual and empleado_actual.get("prestamo_empresa"):
        resultados.append(empleado_actual)

    return resultados


def importar_lista_raya_contpaq(
    archivo,
    user,
    periodo_inicio: date,
    periodo_fin: date,
    quincena_num: int,
) -> dict:
    """
    Aplica cobros de préstamo empresa detectados en Lista de Raya CONTPAQ.
    """
    registros = _parse_xls(archivo)
    log_lines = []
    aplicados = 0
    sin_match = 0
    diferencias = 0

    for reg in registros:
        num = reg["num_empleado"]
        monto_contpaq = reg["prestamo_empresa"]

        try:
            empleado = Empleado.objects.get(codigo=num)
        except Empleado.DoesNotExist:
            log_lines.append(f"[SIN MATCH] Empleado #{num} '{reg['nombre']}' no encontrado en ERP")
            sin_match += 1
            continue

        prestamo = (
            Prestamo.objects.filter(empleado=empleado, estado=Prestamo.ESTADO_ACTIVO)
            .order_by("fecha_solicitud")
            .first()
        )

        if not prestamo:
            log_lines.append(f"[SIN PRESTAMO] {empleado} - cobro CONTPAQ ${monto_contpaq} sin préstamo activo")
            sin_match += 1
            continue

        cuota = (
            prestamo.cuotas.filter(
                estado__in=[PrestamoCuota.ESTADO_PENDIENTE, PrestamoCuota.ESTADO_PARCIAL],
                fecha_quincena__lte=periodo_fin,
            )
            .order_by("numero_quincena")
            .first()
        )

        if not cuota:
            cuota = PrestamoCuota.objects.create(
                prestamo=prestamo,
                numero_quincena=quincena_num,
                fecha_quincena=periodo_fin,
                monto_esperado=prestamo.descuento_quincenal,
                estado=PrestamoCuota.ESTADO_PENDIENTE,
            )
            log_lines.append(f"[CUOTA CREADA] {empleado} Q{quincena_num} - no había cuota proyectada")

        if monto_contpaq != cuota.monto_esperado:
            diferencias += 1
            log_lines.append(
                f"[DIFERENCIA] {empleado} Q{cuota.numero_quincena} - "
                f"esperado ${cuota.monto_esperado}, CONTPAQ ${monto_contpaq}"
            )

        aplicar_cobro_manual(
            cuota,
            monto_contpaq,
            user,
            nota=f"CONTPAQ Q{quincena_num}/{periodo_fin}",
            fuente=PrestamoCuota.FUENTE_CONTPAQ,
        )
        aplicados += 1
        log_lines.append(f"[OK] {empleado} - ${monto_contpaq} aplicado en Q{cuota.numero_quincena}")

    if hasattr(archivo, "seek"):
        archivo.seek(0)

    ImportacionNominaContpaq.objects.create(
        archivo=archivo,
        periodo_inicio=periodo_inicio,
        periodo_fin=periodo_fin,
        quincena_num=quincena_num,
        empleados_leidos=len(registros),
        prestamos_aplicados=aplicados,
        prestamos_sin_match=sin_match,
        diferencias_detectadas=diferencias,
        log="\n".join(log_lines),
        creado_por=user,
    )

    return {
        "empleados_leidos": len(registros),
        "aplicados": aplicados,
        "sin_match": sin_match,
        "diferencias": diferencias,
        "log": log_lines,
    }
