"""Consolidación mensual del gasto/ingreso REAL hacia el presupuesto maestro.

Llena ``LineaPresupuestoMensual.monto_real`` desde las fuentes ERP mapeadas por
``ReglaFuenteRubro``. Convenciones de ``fuente_real``:

- ``AUTO:<TIPO_FUENTE>``  → escrito por este servicio; re-ejecutable.
- ``MANUAL:<username>``   → captura humana; NUNCA se pisa.
- ``AUTO:LEGADO``         → valor migrado de imports previos; re-escribible.

Garantías de escritura:
- La persistencia usa un UPDATE condicional sobre ``fuente_real`` (el valor
  leído debe seguir en la base) — una captura manual concurrente jamás se pisa.
- Si ninguna fuente del rubro tiene datos en el mes, la línea NO se modifica:
  un retraso de Point/nómina no borra el último real consolidado.

El desglose por regla queda en ``metadata["real_breakdown"]`` para drill-down.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from django.db.models import Sum
from django.utils import timezone

from .models import (
    GastoOperativoMensual,
    LineaPresupuestoMensual,
    ReglaFuenteRubro,
)
from .services_presupuesto_maestro import normalize_header_text

AUTO_PREFIX = "AUTO:"
MANUAL_PREFIX = "MANUAL:"
FUENTE_LEGADO = "AUTO:LEGADO"

# fuente_real heredados de imports anteriores → nuevo namespace.
LEGACY_FUENTE_MAP = {
    # Resultados de ventas tecleados del Excel: el POS es autoritativo, re-escribible.
    "PROYECCIO_N_VENTAS_2026_AUTORIZADA": FUENTE_LEGADO,
    # CAPEX confirmado a mano por dirección: protegido.
    "CAPEX_GUAMUCHIL_CONFIRMADO": "MANUAL:legado",
}

NOMINA_CAMPOS_VALIDOS = ("salario_base", "bonos", "total_percepciones", "neto_calculado")
VENTA_POS_CAMPOS_VALIDOS = {"total_venta", "total_venta_neta"}


def es_manual(fuente_real: str) -> bool:
    return str(fuente_real or "").startswith(MANUAL_PREFIX)


def es_escribible(fuente_real: str) -> bool:
    """Solo se escriben líneas vacías o previamente consolidadas por AUTO."""
    valor = str(fuente_real or "").strip()
    return not valor or valor.startswith(AUTO_PREFIX)


@dataclass
class ConsolidacionSummary:
    periodo: date
    version: str
    dry_run: bool
    actualizadas: int = 0
    sin_cambio: int = 0
    protegidas_manual: int = 0
    conflictos_concurrencia: int = 0
    sin_regla: int = 0
    sin_datos_fuente: int = 0
    errores: list[str] = field(default_factory=list)
    detalle: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "periodo": self.periodo.isoformat(),
            "version": self.version,
            "dry_run": self.dry_run,
            "actualizadas": self.actualizadas,
            "sin_cambio": self.sin_cambio,
            "protegidas_manual": self.protegidas_manual,
            "conflictos_concurrencia": self.conflictos_concurrencia,
            "sin_regla": self.sin_regla,
            "sin_datos_fuente": self.sin_datos_fuente,
            "errores": self.errores,
        }


class PresupuestoRealConsolidacionService:
    """Calcula y persiste el monto real por rubro×periodo desde las fuentes.

    Las fuentes se precargan en índices agrupados (una consulta por fuente y
    periodo) y cada regla se resuelve en memoria — el costo no crece con el
    número de rubros.
    """

    def consolidar(
        self,
        *,
        periodo: date,
        version: str = LineaPresupuestoMensual.VERSION_ORIGINAL,
        areas: list[str] | None = None,
        dry_run: bool = False,
    ) -> ConsolidacionSummary:
        periodo = periodo.replace(day=1)
        summary = ConsolidacionSummary(periodo=periodo, version=version, dry_run=dry_run)

        lineas = (
            LineaPresupuestoMensual.objects.filter(periodo=periodo, version=version)
            .select_related("rubro", "rubro__area", "rubro__sucursal")
            .prefetch_related("rubro__reglas_fuente")
        )
        if areas:
            lineas = lineas.filter(rubro__area__codigo__in=areas)

        indices: dict[str, dict] = {}  # un índice por tipo de fuente, perezoso

        for linea in lineas:
            reglas = [
                r
                for r in linea.rubro.reglas_fuente.all()
                if r.activa and r.tipo_fuente != ReglaFuenteRubro.FUENTE_MANUAL
            ]
            if not reglas:
                summary.sin_regla += 1
                continue
            if es_manual(linea.fuente_real):
                summary.protegidas_manual += 1
                continue
            if not es_escribible(linea.fuente_real):
                # fuente legada no migrada: no tocar hasta migrar namespace.
                summary.protegidas_manual += 1
                continue

            total = Decimal("0")
            breakdown: list[dict] = []
            con_datos = False
            try:
                for regla in reglas:
                    monto, hubo_datos = self._monto_regla(regla, periodo, indices)
                    con_datos = con_datos or hubo_datos
                    aporte = (monto * regla.signo).quantize(Decimal("0.01"))
                    total += aporte
                    breakdown.append(
                        {
                            "regla_id": regla.id,
                            "tipo_fuente": regla.tipo_fuente,
                            "monto": str(aporte),
                            "filtros": regla.filtros or {},
                        }
                    )
            except Exception as exc:  # noqa: BLE001 — un rubro mal mapeado no debe tirar el resto
                summary.errores.append(f"rubro={linea.rubro_id} {linea.rubro}: {exc}")
                continue

            if not con_datos:
                # Ninguna fuente tuvo filas este mes: NO tocar el último real,
                # pero dejar la advertencia visible (badge "Fuente sin datos")
                # con la fecha del intento, para que el valor retenido no pase
                # por dato vigente. Escritura condicional: una captura MANUAL
                # concurrente tampoco se pisa aquí.
                summary.sin_datos_fuente += 1
                metadata = dict(linea.metadata or {})
                metadata["sin_datos_fuente"] = True
                metadata["fuente_sin_datos_en"] = timezone.now().isoformat()
                if not dry_run:
                    LineaPresupuestoMensual.objects.filter(
                        pk=linea.pk, fuente_real=linea.fuente_real
                    ).update(metadata=metadata, actualizado_en=timezone.now())
                continue

            tipos = sorted({r.tipo_fuente for r in reglas})
            fuente = AUTO_PREFIX + "+".join(tipos)
            metadata = dict(linea.metadata or {})
            metadata["real_breakdown"] = breakdown
            metadata["consolidado_en"] = timezone.now().isoformat()
            metadata.pop("sin_datos_fuente", None)

            if linea.monto_real == total and linea.fuente_real == fuente:
                summary.sin_cambio += 1
                continue

            if not dry_run and not self._escribir_linea(linea, total, fuente, metadata):
                summary.conflictos_concurrencia += 1
                continue

            summary.actualizadas += 1
            summary.detalle.append(
                {
                    "linea_id": linea.id,
                    "rubro": str(linea.rubro),
                    "anterior": str(linea.monto_real) if linea.monto_real is not None else None,
                    "nuevo": str(total),
                    "fuente": fuente,
                }
            )

        return summary

    @staticmethod
    def _escribir_linea(
        linea: LineaPresupuestoMensual, total: Decimal, fuente: str, metadata: dict
    ) -> bool:
        """UPDATE condicional: solo escribe si fuente_real no cambió desde la lectura.

        Si una usuaria capturó (MANUAL:*) entre la lectura y este punto, el
        filtro no coincide, no se escribe nada y se reporta como conflicto.
        """
        actualizadas = LineaPresupuestoMensual.objects.filter(
            pk=linea.pk, fuente_real=linea.fuente_real
        ).update(
            monto_real=total,
            fuente_real=fuente,
            metadata=metadata,
            actualizado_en=timezone.now(),
        )
        return actualizadas == 1

    # ------------------------------------------------------------------ #
    # Fuentes (índices agrupados, una consulta por fuente y periodo)      #
    # ------------------------------------------------------------------ #

    def _monto_regla(
        self, regla: ReglaFuenteRubro, periodo: date, indices: dict
    ) -> tuple[Decimal, bool]:
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO:
            if "gasto" not in indices:
                indices["gasto"] = self._build_gasto_index(periodo)
            return self._monto_gasto_operativo(regla, indices["gasto"])
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_NOMINA:
            if "nomina" not in indices:
                indices["nomina"] = self._build_nomina_index(periodo)
            return self._monto_nomina(regla, indices["nomina"])
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_VENTA_POS:
            if "ventas" not in indices:
                indices["ventas"] = self._build_ventas_index(periodo)
            return self._monto_venta_pos(regla, indices["ventas"])
        raise ValueError(f"tipo_fuente no soportado aún: {regla.tipo_fuente}")

    @staticmethod
    def _build_gasto_index(periodo: date) -> list[dict]:
        """Gasto REAL del mes agrupado por categoría × centro (con su sucursal/tipo)."""
        return list(
            GastoOperativoMensual.objects.filter(
                periodo=periodo, tipo_dato=GastoOperativoMensual.TIPO_DATO_REAL
            )
            .values(
                "categoria_gasto_id",
                "centro_costo_id",
                "centro_costo__sucursal_id",
                "centro_costo__tipo",
            )
            .annotate(monto=Sum("monto"))
        )

    def _monto_gasto_operativo(
        self, regla: ReglaFuenteRubro, gasto_index: list[dict]
    ) -> tuple[Decimal, bool]:
        if regla.categoria_gasto_id is None:
            raise ValueError("regla GASTO_OPERATIVO sin categoria_gasto")
        sucursal = regla.sucursal_efectiva()
        sucursal_id = sucursal.id if sucursal is not None else None
        centro_tipo = (regla.filtros or {}).get("centro_tipo")

        total = Decimal("0")
        hubo_datos = False
        for fila in gasto_index:
            if fila["categoria_gasto_id"] != regla.categoria_gasto_id:
                continue
            if regla.centro_costo_id:
                if fila["centro_costo_id"] != regla.centro_costo_id:
                    continue
            else:
                if sucursal_id is not None and fila["centro_costo__sucursal_id"] != sucursal_id:
                    continue
                if centro_tipo and fila["centro_costo__tipo"] != centro_tipo:
                    continue
            total += fila["monto"] or Decimal("0")
            hubo_datos = True
        return (total, hubo_datos)

    @staticmethod
    def _build_nomina_index(periodo: date) -> list[dict]:
        """Nómina cerrada/pagada del mes agrupada por departamento × sucursal.

        Un periodo de nómina pertenece al mes donde termina (fecha_fin).
        Trae los cuatro campos monetarios de una vez.
        """
        from rrhh.models import NominaLinea, NominaPeriodo

        agregados = {campo: Sum(campo) for campo in NOMINA_CAMPOS_VALIDOS}
        return list(
            NominaLinea.objects.filter(
                periodo__fecha_fin__year=periodo.year,
                periodo__fecha_fin__month=periodo.month,
                periodo__estatus__in=[NominaPeriodo.ESTATUS_CERRADA, NominaPeriodo.ESTATUS_PAGADA],
            )
            .values("empleado__departamento", "empleado__sucursal_ref_id")
            .annotate(**agregados)
        )

    def _monto_nomina(self, regla: ReglaFuenteRubro, nomina_index: list[dict]) -> tuple[Decimal, bool]:
        filtros = regla.filtros or {}
        campo = filtros.get("campo_monto", "total_percepciones")
        if campo not in NOMINA_CAMPOS_VALIDOS:
            raise ValueError(f"campo_monto de nómina inválido: {campo}")
        departamento = str(filtros.get("departamento") or "").strip().upper()
        sucursal = regla.sucursal_efectiva()
        sucursal_id = sucursal.id if sucursal is not None else None

        total = Decimal("0")
        hubo_datos = False
        for fila in nomina_index:
            if departamento and fila["empleado__departamento"] != departamento:
                continue
            if sucursal_id is not None and fila["empleado__sucursal_ref_id"] != sucursal_id:
                continue
            total += fila[campo] or Decimal("0")
            hubo_datos = True
        return (total, hubo_datos)

    @staticmethod
    def _build_ventas_index(periodo: date) -> dict:
        """Suma mensual de ventas POS por (sucursal, categoría, producto) normalizados.

        Se agrega en Python porque los nombres de categoría/producto del POS
        traen acentos y mayúsculas inconsistentes frente a los conceptos del
        presupuesto.
        """
        from pos_bridge.models.sales_pipeline import PointSalesDailyProductFact

        index: dict[tuple, dict[str, Decimal]] = {}
        rows = (
            PointSalesDailyProductFact.objects.filter(
                sale_date__year=periodo.year, sale_date__month=periodo.month
            )
            .values("branch__erp_branch_id", "categoria", "producto_nombre_historico")
            .annotate(venta=Sum("total_venta"), venta_neta=Sum("total_venta_neta"))
        )
        for row in rows:
            key = (
                row["branch__erp_branch_id"],
                normalize_header_text(row["categoria"]),
                normalize_header_text(row["producto_nombre_historico"]),
            )
            bucket = index.setdefault(key, {"total_venta": Decimal("0"), "total_venta_neta": Decimal("0")})
            bucket["total_venta"] += row["venta"] or Decimal("0")
            bucket["total_venta_neta"] += row["venta_neta"] or Decimal("0")
        return index

    def _monto_venta_pos(self, regla: ReglaFuenteRubro, ventas_index: dict) -> tuple[Decimal, bool]:
        filtros = regla.filtros or {}
        campo = filtros.get("campo_monto", "total_venta")
        if campo not in VENTA_POS_CAMPOS_VALIDOS:
            raise ValueError(f"campo_monto de ventas inválido: {campo}")
        categoria = normalize_header_text(filtros.get("categoria_pos", ""))
        producto = normalize_header_text(filtros.get("producto_pos", ""))
        sucursal = regla.sucursal_efectiva()
        sucursal_id = sucursal.id if sucursal is not None else None

        total = Decimal("0")
        hubo_datos = False
        for (branch_id, cat, prod), montos in (ventas_index or {}).items():
            if sucursal_id is not None and branch_id != sucursal_id:
                continue
            if categoria and cat != categoria:
                continue
            if producto and prod != producto:
                continue
            total += montos[campo]
            hubo_datos = True
        return (total, hubo_datos)


def migrar_fuentes_legadas(*, dry_run: bool = False) -> dict[str, int]:
    """Mueve los fuente_real heredados al namespace AUTO:/MANUAL:."""
    resultado: dict[str, int] = {}
    for legado, nuevo in LEGACY_FUENTE_MAP.items():
        qs = LineaPresupuestoMensual.objects.filter(fuente_real=legado)
        resultado[legado] = qs.count()
        if not dry_run:
            qs.update(fuente_real=nuevo)
    return resultado
