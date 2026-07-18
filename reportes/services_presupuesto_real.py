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
BONO_PROD_CAMPOS_VALIDOS = (
    "total_a_pagar",
    "monto_asistencia",
    "monto_puntualidad",
    "monto_produccion",
    "monto_uniforme",
    "monto_premio_embetunado",
)
BONO_VENTAS_CAMPOS_VALIDOS = (
    "total_a_pagar",
    "monto_asistencia",
    "monto_puntualidad",
    "monto_bono_entregas",
    "bono_ventas",
)


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
            metadata.pop("fuente_sin_datos_en", None)

            # "Sin cambio" solo si tampoco hay una advertencia de fuente sin
            # datos pendiente de limpiar — si la fuente se recuperó con el
            # mismo importe, hay que persistir la limpieza del badge.
            tenia_advertencia = bool((linea.metadata or {}).get("sin_datos_fuente"))
            if linea.monto_real == total and linea.fuente_real == fuente and not tenia_advertencia:
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
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_BONO_PRODUCCION:
            if "bono_prod" not in indices:
                indices["bono_prod"] = self._build_bono_prod_index(periodo)
            return self._monto_bono(regla, indices["bono_prod"], BONO_PROD_CAMPOS_VALIDOS)
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_BONO_VENTAS:
            if "bono_ventas" not in indices:
                indices["bono_ventas"] = self._build_bono_ventas_index(periodo)
            return self._monto_bono(regla, indices["bono_ventas"], BONO_VENTAS_CAMPOS_VALIDOS)
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_CONSUMO_MP:
            if "consumo" not in indices:
                indices["consumo"] = self._build_consumo_index(periodo)
            return self._monto_consumo_mp(regla, indices["consumo"], periodo)
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_MANTENIMIENTO_UNIDAD:
            if "mant_unidad" not in indices:
                indices["mant_unidad"] = self._build_mant_unidad_index(periodo)
            return self._monto_mantenimiento_unidad(regla, indices["mant_unidad"])
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_COMBUSTIBLE_UNIDAD:
            if "combustible" not in indices:
                indices["combustible"] = self._build_combustible_index(periodo)
            return self._monto_combustible_unidad(regla, indices["combustible"], periodo)
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_MANTENIMIENTO_EQUIPO:
            if "mant_equipo" not in indices:
                indices["mant_equipo"] = self._build_mant_equipo_index(periodo)
            return self._monto_mantenimiento_equipo(regla, indices["mant_equipo"], periodo)
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_COSTO_REVENTA:
            if "costo_reventa" not in indices:
                indices["costo_reventa"] = self._build_costo_reventa_index(periodo)
            return self._monto_costo_reventa(regla, indices["costo_reventa"], periodo)
        if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_MERMA_PRODUCTO:
            if "merma_producto" not in indices:
                indices["merma_producto"] = self._build_merma_producto_total(periodo)
            return self._monto_merma_producto(regla, indices["merma_producto"], periodo)
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
        """Suma las ventas POS asignadas a la regla.

        La asignación es EXPLÍCITA (la calcula el seed y queda auditada en
        filtros): ``categoria_pos`` con lista ``productos_pos`` (nombres POS
        exactos), o solo ``categoria_pos`` para sumar la categoría completa.
        Se acepta el legado ``producto_pos`` (string único).
        """
        filtros = regla.filtros or {}
        campo = filtros.get("campo_monto", "total_venta")
        if campo not in VENTA_POS_CAMPOS_VALIDOS:
            raise ValueError(f"campo_monto de ventas inválido: {campo}")
        clasificaciones = filtros.get("clasificacion_catalogo") or []
        excluir_clasificados = bool(filtros.get("excluir_clasificados"))
        if clasificaciones or excluir_clasificados:
            # "Complementos" = productos del catálogo curado (PointProductCategory:
            # REVENTA / SERVICIO_ACCESORIO / TOPPING). El renglón de postres usa
            # excluir_clasificados para no contarlos doble.
            productos_clasificados = self._productos_clasificados(set(clasificaciones) or None)
        if filtros.get("total_empresa"):
            # Venta total Point (renglón Ingresos del P&L): todo el índice.
            total = Decimal("0")
            hubo_datos = False
            for (branch_id, cat, prod), montos in (ventas_index or {}).items():
                if excluir_clasificados and prod in self._productos_clasificados(None):
                    continue
                total += montos[campo]
                hubo_datos = True
            return (total, hubo_datos)
        if clasificaciones:
            total = Decimal("0")
            hubo_datos = False
            for (branch_id, cat, prod), montos in (ventas_index or {}).items():
                if prod in productos_clasificados:
                    total += montos[campo]
                    hubo_datos = True
            return (total, hubo_datos)
        categoria = normalize_header_text(filtros.get("categoria_pos", ""))
        productos_raw = filtros.get("productos_pos")
        if not productos_raw and filtros.get("producto_pos"):
            productos_raw = [filtros["producto_pos"]]
        productos = {normalize_header_text(p) for p in (productos_raw or []) if str(p).strip()}
        if not categoria and not productos:
            # Regla sin asignación POS (el seed no encontró match): sin datos.
            return (Decimal("0"), False)
        sucursal = regla.sucursal_efectiva()
        sucursal_id = sucursal.id if sucursal is not None else None

        total = Decimal("0")
        hubo_datos = False
        for (branch_id, cat, prod), montos in (ventas_index or {}).items():
            if sucursal_id is not None and branch_id != sucursal_id:
                continue
            if categoria and cat != categoria:
                continue
            if productos and prod not in productos:
                continue
            total += montos[campo]
            hubo_datos = True
        return (total, hubo_datos)


    def _productos_clasificados(self, categorias: set[str] | None) -> set[str]:
        """Nombres (normalizados) de productos del catálogo curado de reventa/
        accesorios/toppings (pos_bridge.PointProductCategory). Cacheado por
        instancia; con ``categorias=None`` regresa TODOS los clasificados."""
        cache = getattr(self, "_clasificados_cache", None)
        if cache is None:
            from pos_bridge.models import PointProductCategory

            cache = self._clasificados_cache = {}
            for fila in PointProductCategory.objects.all().values("nombre", "category"):
                cache.setdefault(fila["category"], set()).add(normalize_header_text(fila["nombre"]))
        if categorias is None:
            return set().union(*cache.values()) if cache else set()
        return set().union(*(cache.get(c, set()) for c in categorias)) if cache else set()

    @staticmethod
    def _build_bono_prod_index(periodo: date) -> list[dict]:
        """Bonos de producción del mes por sucursal del empleado (todos los montos)."""
        from bonos_produccion.models import BonoProduccionEmpleado

        agregados = {campo: Sum(campo) for campo in BONO_PROD_CAMPOS_VALIDOS}
        return list(
            BonoProduccionEmpleado.objects.filter(
                periodo__mes=periodo.month, periodo__anio=periodo.year
            )
            .values("empleado__sucursal_ref_id")
            .annotate(**agregados)
        )

    @staticmethod
    def _build_bono_ventas_index(periodo: date) -> list[dict]:
        """Bonos de ventas del mes por sucursal (FK directa)."""
        from bonos_ventas.models import BonoVentasEmpleado

        agregados = {campo: Sum(campo) for campo in BONO_VENTAS_CAMPOS_VALIDOS}
        return list(
            BonoVentasEmpleado.objects.filter(
                periodo__mes=periodo.month, periodo__anio=periodo.year
            )
            .values("sucursal_id")
            .annotate(**agregados)
        )

    def _monto_bono(
        self, regla: ReglaFuenteRubro, bono_index: list[dict], campos_validos: tuple
    ) -> tuple[Decimal, bool]:
        filtros = regla.filtros or {}
        campo = filtros.get("campo_monto", "total_a_pagar")
        if campo not in campos_validos:
            raise ValueError(f"campo_monto de bono inválido: {campo}")
        sucursal = regla.sucursal_efectiva()
        sucursal_id = sucursal.id if sucursal is not None else None

        total = Decimal("0")
        hubo_datos = False
        for fila in bono_index:
            fila_sucursal = fila.get("sucursal_id", fila.get("empleado__sucursal_ref_id"))
            if sucursal_id is not None and fila_sucursal != sucursal_id:
                continue
            total += fila[campo] or Decimal("0")
            hubo_datos = True
        return (total, hubo_datos)

    @staticmethod
    def _build_consumo_index(periodo: date) -> dict[int, Decimal]:
        """Costo real de consumo del mes por insumo."""
        from inventario.models import ConsumoInsumoMensual

        return {
            fila["insumo_id"]: fila["costo"] or Decimal("0")
            for fila in ConsumoInsumoMensual.objects.filter(periodo=periodo)
            .values("insumo_id")
            .annotate(costo=Sum("costo_real"))
        }

    def _monto_consumo_mp(
        self, regla: ReglaFuenteRubro, consumo_index: dict[int, Decimal], periodo: date
    ) -> tuple[Decimal, bool]:
        filtros = regla.filtros or {}
        desde = str(filtros.get("desde") or "")
        if desde and periodo.strftime("%Y-%m") < desde:
            # El consumo del ERP no es confiable antes de esta fecha (meses
            # incompletos y ajustes erróneos): se reporta "sin datos" para que
            # la línea conserve el valor legado del Excel.
            return (Decimal("0"), False)
        if filtros.get("total_empresa"):
            # Consumo de MP de toda la empresa (renglón Costos del P&L).
            if not consumo_index:
                return (Decimal("0"), False)
            return (sum(consumo_index.values(), Decimal("0")), True)
        insumo_id = filtros.get("insumo_id")
        if not insumo_id:
            raise ValueError("regla CONSUMO_MP sin insumo_id en filtros")
        if int(insumo_id) not in consumo_index:
            return (Decimal("0"), False)
        return (consumo_index[int(insumo_id)], True)


    @staticmethod
    def _build_merma_producto_total(periodo: date) -> tuple[Decimal, bool]:
        """Valor de la merma del mes según el registro madre de Point
        (control.MermaPOS, sync /Mermas/get_mermas): cubre todas las
        sucursales, CEDIS y almacén. Se valúa a costo vigente de la receta
        (capa MP del costeo). El módulo PWA de mermas es el rastreo logístico
        del mismo evento (trae ticket_point) — usarlo aquí duplicaría."""
        from control.models import MermaPOS

        total = Decimal("0")
        hubo_datos = False
        lineas = MermaPOS.objects.filter(
            fecha__year=periodo.year, fecha__month=periodo.month
        ).select_related("receta")
        for linea in lineas:
            hubo_datos = True
            if not linea.receta_id:
                continue
            # Recetas de lote traen rendimiento (costo/pieza = total/rendimiento);
            # en recetas de producto terminado el costo total YA es por pieza.
            costo_unitario = linea.receta.costo_por_unidad_rendimiento
            if not costo_unitario or costo_unitario <= 0:
                costo_unitario = linea.receta.costo_total_estimado_decimal
            if not costo_unitario or costo_unitario <= 0:
                continue
            total += Decimal(str(linea.cantidad)) * costo_unitario
        return (total.quantize(Decimal("0.01")), hubo_datos)

    def _monto_merma_producto(
        self, regla: ReglaFuenteRubro, merma_total: tuple[Decimal, bool], periodo: date
    ) -> tuple[Decimal, bool]:
        if self._fuera_de_vigencia(regla, periodo):
            return (Decimal("0"), False)
        return merma_total

    @staticmethod
    def _build_mant_unidad_index(periodo: date) -> dict[str, Decimal]:
        """Costo de servicio del mes por unidad vehicular (logística).

        Fuente: logistica.ReporteUnidad.costo_servicio — la base ligada de la
        flotilla (unidad + reporte + costo) que pidió dirección conectar.
        """
        from logistica.models import ReporteUnidad

        return {
            fila["unidad__codigo"]: fila["costo"] or Decimal("0")
            for fila in ReporteUnidad.objects.filter(
                fecha_reporte__year=periodo.year,
                fecha_reporte__month=periodo.month,
                costo_servicio__isnull=False,
                costo_servicio__gt=0,
            )
            .values("unidad__codigo")
            .annotate(costo=Sum("costo_servicio"))
            if fila["unidad__codigo"]
        }

    def _monto_mantenimiento_unidad(
        self, regla: ReglaFuenteRubro, mant_index: dict[str, Decimal]
    ) -> tuple[Decimal, bool]:
        codigo = str((regla.filtros or {}).get("unidad_codigo") or "").strip()
        if not codigo:
            raise ValueError("regla MANTENIMIENTO_UNIDAD sin unidad_codigo en filtros")
        if codigo not in mant_index:
            return (Decimal("0"), False)
        return (mant_index[codigo], True)


    @staticmethod
    def _fuera_de_vigencia(regla: ReglaFuenteRubro, periodo: date) -> bool:
        """Filtro ``desde`` (YYYY-MM): antes de esa fecha la fuente reporta
        "sin datos" y la línea conserva el legado del Excel."""
        desde = str((regla.filtros or {}).get("desde") or "")
        return bool(desde) and periodo.strftime("%Y-%m") < desde

    @staticmethod
    def _build_combustible_index(periodo: date) -> dict[str, Decimal]:
        """Cargas de combustible del mes por unidad (bitácora de logística)."""
        from logistica.models import CargaCombustibleUnidad

        return {
            fila["unidad__codigo"]: fila["importe"] or Decimal("0")
            for fila in CargaCombustibleUnidad.objects.filter(
                fecha_registro__year=periodo.year,
                fecha_registro__month=periodo.month,
            )
            .values("unidad__codigo")
            .annotate(importe=Sum("importe_total"))
            if fila["unidad__codigo"]
        }

    def _monto_combustible_unidad(
        self, regla: ReglaFuenteRubro, index: dict[str, Decimal], periodo: date
    ) -> tuple[Decimal, bool]:
        if self._fuera_de_vigencia(regla, periodo):
            return (Decimal("0"), False)
        unidades = [str(u).strip() for u in (regla.filtros or {}).get("unidades") or [] if str(u).strip()]
        if not unidades:
            raise ValueError("regla COMBUSTIBLE_UNIDAD sin lista de unidades en filtros")
        total = Decimal("0")
        hubo_datos = False
        for codigo in unidades:
            if codigo in index:
                total += index[codigo]
                hubo_datos = True
        return (total, hubo_datos)

    @staticmethod
    def _build_mant_equipo_index(periodo: date) -> list[dict]:
        """Órdenes de mantenimiento del mes (activos) con costo, por sucursal
        y ubicación del activo."""
        from django.db.models import F as _F

        from activos.models import OrdenMantenimiento

        return list(
            OrdenMantenimiento.objects.filter(
                creado_en__year=periodo.year, creado_en__month=periodo.month
            )
            .annotate(total=_F("costo_repuestos") + _F("costo_mano_obra") + _F("costo_otros"))
            .values("activo_ref__sucursal__codigo", "activo_ref__ubicacion")
            .annotate(monto=Sum("total"))
        )

    def _monto_mantenimiento_equipo(
        self, regla: ReglaFuenteRubro, index: list[dict], periodo: date
    ) -> tuple[Decimal, bool]:
        if self._fuera_de_vigencia(regla, periodo):
            return (Decimal("0"), False)
        filtros = regla.filtros or {}
        sucursal_codigo = str(filtros.get("sucursal_codigo") or "").strip()
        if not sucursal_codigo and filtros.get("por_sucursal"):
            # La regla hereda la sucursal de su rubro (una fila del CSV cubre
            # los 8 rubros por sucursal, cada uno con sus propios activos).
            sucursal = regla.sucursal_efectiva()
            sucursal_codigo = sucursal.codigo if sucursal is not None else ""
            if not sucursal_codigo:
                return (Decimal("0"), False)
        solo_produccion = bool(filtros.get("ubicaciones_produccion"))
        if not sucursal_codigo and not solo_produccion:
            raise ValueError("regla MANTENIMIENTO_EQUIPO sin sucursal_codigo ni ubicaciones_produccion")
        total = Decimal("0")
        hubo_datos = False
        for fila in index:
            ubicacion = str(fila.get("activo_ref__ubicacion") or "").upper()
            es_produccion = "PRODUCCION" in ubicacion or "HORNOS" in ubicacion or fila.get("activo_ref__sucursal__codigo") == "CEDIS"
            if solo_produccion != es_produccion:
                continue
            if sucursal_codigo and fila.get("activo_ref__sucursal__codigo") != sucursal_codigo:
                continue
            total += fila["monto"] or Decimal("0")
            hubo_datos = True
        return (total, hubo_datos)


    def _build_costo_reventa_index(self, periodo: date) -> dict:
        """Costo de los complementos vendidos en el mes: unidades vendidas de
        cada producto del catálogo curado × su costo de reventa (histórico
        mensual si existe; si no, la vigencia más reciente)."""
        from pos_bridge.models import PointProduct, PointProductCategory
        from pos_bridge.models.sales_pipeline import PointSalesDailyProductFact

        clasificados = {
            normalize_header_text(n)
            for n in PointProductCategory.objects.values_list("nombre", flat=True)
        }
        ventas = (
            PointSalesDailyProductFact.objects.filter(
                sale_date__year=periodo.year,
                sale_date__month=periodo.month,
                point_product__isnull=False,
            )
            .values("point_product_id", "point_product__name")
            .annotate(unidades=Sum("total_cantidad"))
        )
        total = Decimal("0")
        con_datos = False
        sin_costo = 0
        ids = [
            v["point_product_id"]
            for v in ventas
            if normalize_header_text(v["point_product__name"]) in clasificados
        ]
        productos = {
            p.id: p
            for p in PointProduct.objects.filter(id__in=ids).prefetch_related(
                "costos_reventa", "costos_reventa_historicos_mensuales"
            )
        }
        corte = periodo.replace(day=28)
        for v in ventas:
            producto = productos.get(v["point_product_id"])
            if producto is None:
                continue
            historico = next(
                (h for h in producto.costos_reventa_historicos_mensuales.all() if h.periodo == periodo),
                None,
            )
            if historico is not None and historico.costo_promedio:
                costo = Decimal(historico.costo_promedio)
            else:
                vigencias = [
                    c for c in producto.costos_reventa.all() if c.fecha_vigencia <= corte
                ]
                if not vigencias:
                    sin_costo += 1
                    continue
                costo = Decimal(max(vigencias, key=lambda c: c.fecha_vigencia).costo_unitario)
            total += (v["unidades"] or Decimal("0")) * costo
            con_datos = True
        return {"total": total.quantize(Decimal("0.01")), "con_datos": con_datos, "sin_costo": sin_costo}

    def _monto_costo_reventa(
        self, regla: ReglaFuenteRubro, index: dict, periodo: date
    ) -> tuple[Decimal, bool]:
        if self._fuera_de_vigencia(regla, periodo):
            return (Decimal("0"), False)
        return (index["total"], index["con_datos"])


def limpiar_reales_sin_asignacion(*, dry_run: bool = False) -> int:
    """Anula reales AUTO de rubros cuya regla VENTA_POS ya no tiene asignación.

    Caso real: un rubro consolidó con una asignación que luego se detectó
    incorrecta (p.ej. una categoría solapada anulada por exclusividad). La
    regla queda "sin asignación" y la protección de fuente-vacía retendría el
    valor viejo para siempre. Aquí se limpia explícitamente: la línea vuelve a
    "pendiente" (nunca toca MANUAL:*).
    """
    limpiadas = 0
    reglas = ReglaFuenteRubro.objects.filter(
        tipo_fuente=ReglaFuenteRubro.FUENTE_VENTA_POS, activa=True
    ).select_related("rubro")
    rubros_sin_asignacion = [
        regla.rubro_id
        for regla in reglas
        if not (regla.filtros or {}).get("total_empresa")
        and not (regla.filtros or {}).get("categoria_pos")
        and not (regla.filtros or {}).get("productos_pos")
        and not (regla.filtros or {}).get("producto_pos")
    ]
    lineas = LineaPresupuestoMensual.objects.filter(
        rubro_id__in=rubros_sin_asignacion,
        fuente_real__startswith=AUTO_PREFIX,
        monto_real__isnull=False,
    )
    for linea in lineas:
        limpiadas += 1
        if dry_run:
            continue
        metadata = dict(linea.metadata or {})
        metadata.pop("real_breakdown", None)
        metadata.pop("sin_datos_fuente", None)
        metadata.pop("fuente_sin_datos_en", None)
        metadata["limpiado_sin_asignacion_en"] = timezone.now().isoformat()
        LineaPresupuestoMensual.objects.filter(pk=linea.pk, fuente_real=linea.fuente_real).update(
            monto_real=None, fuente_real="", metadata=metadata, actualizado_en=timezone.now()
        )
    return limpiadas


def migrar_fuentes_legadas(*, dry_run: bool = False) -> dict[str, int]:
    """Mueve los fuente_real heredados al namespace AUTO:/MANUAL:."""
    resultado: dict[str, int] = {}
    for legado, nuevo in LEGACY_FUENTE_MAP.items():
        qs = LineaPresupuestoMensual.objects.filter(fuente_real=legado)
        resultado[legado] = qs.count()
        if not dry_run:
            qs.update(fuente_real=nuevo)
    return resultado
