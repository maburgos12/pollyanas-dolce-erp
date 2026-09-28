"""Etapa 2 de la separación de Bamoa: mueve la operación.

La etapa 1 separó el catálogo, Point, los centros de costo y el presupuesto.
Aquí va lo demás: ventas, producción, inventario, asistencia, mermas, alertas y
todo lo que quedó colgando del registro de Crucero.

El corte es la apertura de Bamoa. Lo diario corta el 14-jul-2026, su primera
venta tras 23 días sin ninguna; lo mensual corta el 1-jul, porque un periodo no
se parte a la mitad.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from core.models import Sucursal

CORTE_DIARIO = "2026-07-14"
CORTE_MENSUAL = "2026-07-01"

# (tabla, columna de sucursal, columna de fecha, corte)
POR_FECHA = [
    ("reportes_factventadiaria", "sucursal_id", "fecha", CORTE_DIARIO),
    ("recetas_ventahistorica", "sucursal_id", "fecha", CORTE_DIARIO),
    ("ventas_autoritativas_point", "branch_id", "sale_date", CORTE_DIARIO),
    ("reportes_factproducciondiaria", "sucursal_id", "fecha", CORTE_DIARIO),
    ("reportes_factinventariodiario", "sucursal_id", "fecha", CORTE_DIARIO),
    ("control_ventapos", "sucursal_id", "fecha", CORTE_DIARIO),
    ("control_mermapos", "sucursal_id", "fecha", CORTE_DIARIO),
    ("reportes_alert", "sucursal_id", "fecha", CORTE_DIARIO),
    ("reportes_forecastcalibrationprofile", "sucursal_id", "reference_date", CORTE_DIARIO),
    ("logistica_rutacargachecklistlinea", "erp_destination_branch_id", "validado_en", CORTE_DIARIO),
    ("pos_bridge_transfer_lines", "erp_destination_branch_id", "registered_at", CORTE_DIARIO),
    ("pos_bridge_transfer_lines", "erp_origin_branch_id", "registered_at", CORTE_DIARIO),
    ("pos_bridge_waste_lines", "erp_branch_id", "movement_at", CORTE_DIARIO),
    ("pos_bridge_production_lines", "erp_branch_id", "production_date", CORTE_DIARIO),
    ("pos_bridge_conversion_lines", "erp_branch_id", "movement_at", CORTE_DIARIO),
    ("rrhh_asistenciaempleado", "sucursal_id", "fecha", CORTE_DIARIO),
    ("reportes_productionorder", "sucursal_id", "fecha", CORTE_DIARIO),
    ("recetas_solicitudreabastocedis", "sucursal_id", "fecha_operacion", CORTE_DIARIO),
    ("operacion_registrohigiene", "sucursal_id", "fecha", CORTE_DIARIO),
    ("visitas_sucursal_visitasucursal", "sucursal_id", "fecha_programada", CORTE_DIARIO),
    ("fallas_reportefalla", "sucursal_id", "fecha_reporte", CORTE_DIARIO),
    # aplicado_en viene en nulo; la captura es la que fecha el movimiento
    ("mermas_ordenajustepoint", "sucursal_id", "creado_en", CORTE_DIARIO),
    ("mermas_mermainsumo", "sucursal_id", "creado_en", CORTE_DIARIO),
    ("mermas_mermaregistro", "sucursal_id", "iniciado_en", CORTE_DIARIO),
    ("conciliacion_cfdisucursalresolucion", "sucursal_id", "creado_en", CORTE_DIARIO),
    ("proyecciones_proyeccionproduccion", "sucursal_id", "periodo", CORTE_MENSUAL),
    ("reportes_stockmensualsucursal", "sucursal_id", "periodo", CORTE_MENSUAL),
    ("reportes_productosucursalcontribucionmensual", "sucursal_id", "periodo", CORTE_MENSUAL),
    ("control_devolucionsucursalmatriz", "sucursal_origen_id", "periodo", CORTE_MENSUAL),
    ("control_mermamensualsucursal", "sucursal_id", "periodo", CORTE_MENSUAL),
    ("rentabilidad_sucursalrentabilidad", "sucursal_id", "periodo", CORTE_MENSUAL),
    ("rrhh_vacanterrhh", "sucursal_id", "fecha_solicitada", CORTE_DIARIO),
]

# Quedan fuera a propósito, porque no los decide una fecha:
#   activos_activo — 9 equipos; si se mudaron del local viejo al nuevo lo sabe
#     operaciones, no la base.
#   ventas_pronosticoguardado_sucursales — tabla puente sin fecha propia.
# También conviene revisar los patrones fiscales: al pasarlos a Bamoa, un CFDI
# histórico que diga «Crucero» resolverá a la tienda nueva.

# Tablas cuya fecha vive en su padre: (tabla, columna, condición SQL)
POR_PADRE = [
    (
        "rrhh_nominalinea", "sucursal_snapshot_id",
        "periodo_id IN (SELECT id FROM rrhh_nominaperiodo WHERE fecha_inicio >= %(mensual)s)",
    ),
    (
        "reportes_detallecedulaimss", "sucursal_id",
        "documento_id IN (SELECT d.id FROM reportes_documentocedulaimss d "
        "JOIN reportes_expedientecedulaimss e ON e.id=d.expediente_id WHERE e.periodo >= %(mensual)s)",
    ),
    (
        "reportes_distribucionisnempleado", "sucursal_id",
        "expediente_id IN (SELECT id FROM reportes_expedientecedulaimss WHERE periodo >= %(mensual)s)",
    ),
    (
        "bonos_ventas_bonoventasempleado", "sucursal_id",
        "periodo_id IN (SELECT id FROM bonos_ventas_configbonoperiodo "
        "WHERE (anio > 2026) OR (anio = 2026 AND mes >= 7))",
    ),
]

# Describen la tienda de hoy, no un periodo: pasan completas a Bamoa.
ESTADO_ACTUAL = [
    ("rrhh_empleado", "sucursal_ref_id", "empleados asignados"),
    ("core_userprofile", "sucursal_id", "perfiles de usuario"),
    ("logistica_puntologistico", "sucursal_id", "punto logístico"),
    ("conciliacion_sucursalidentificadorfiscal", "sucursal_id", "patrones fiscales"),
]


class Command(BaseCommand):
    help = "Mueve la operación de Crucero a Bamoa a partir de su apertura (etapa 2)."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        try:
            crucero = Sucursal.objects.get(codigo="CRUCERO")
            bamoa = Sucursal.objects.get(codigo="BAMOA")
        except Sucursal.DoesNotExist as exc:
            raise CommandError("Falta la etapa 1: no existen las sucursales CRUCERO y BAMOA.") from exc

        params = {"origen": crucero.pk, "destino": bamoa.pk, "mensual": CORTE_MENSUAL}
        ausentes = self._tablas_ausentes()
        if ausentes:
            self.stdout.write(self.style.WARNING(f"  tablas no presentes, se omiten: {', '.join(ausentes)}"))
        total = 0
        with transaction.atomic(), connection.cursor() as cur:
            total += self._bloque(cur, "POR FECHA", self._sql_fecha(), params)
            total += self._bloque(cur, "POR PERIODO DEL PADRE", self._sql_padre(), params)
            total += self._bloque(cur, "ESTADO ACTUAL DE LA TIENDA", self._sql_estado(), params)
            self.stdout.write("")
            self.stdout.write(f"  renglones movidos a Bamoa: {total:,}")
            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))

    # ------------------------------------------------------------------ #

    @staticmethod
    def _tablas_ausentes() -> list[str]:
        """Una migración que nombra 30 tablas comprueba que existan antes de tocar nada."""
        nombradas = {t for t, *_ in POR_FECHA} | {t for t, *_ in POR_PADRE} | {t for t, *_ in ESTADO_ACTUAL}
        existentes = set(connection.introspection.table_names())
        return sorted(nombradas - existentes)

    def _sql_fecha(self):
        for tabla, col, fecha, corte in POR_FECHA:
            if tabla not in connection.introspection.table_names():
                continue
            yield (
                f"{tabla}.{col}",
                f'UPDATE "{tabla}" SET "{col}" = %(destino)s '
                f'WHERE "{col}" = %(origen)s AND "{fecha}" >= \'{corte}\'',
            )

    def _sql_padre(self):
        for tabla, col, condicion in POR_PADRE:
            if tabla not in connection.introspection.table_names():
                continue
            yield (
                f"{tabla}.{col}",
                f'UPDATE "{tabla}" SET "{col}" = %(destino)s '
                f'WHERE "{col}" = %(origen)s AND {condicion}',
            )

    def _sql_estado(self):
        for tabla, col, etiqueta in ESTADO_ACTUAL:
            if tabla not in connection.introspection.table_names():
                continue
            yield (
                f"{tabla} ({etiqueta})",
                f'UPDATE "{tabla}" SET "{col}" = %(destino)s WHERE "{col}" = %(origen)s',
            )

    def _bloque(self, cur, titulo, generador, params):
        self.stdout.write("")
        self.stdout.write(titulo)
        total = 0
        for etiqueta, sql in generador:
            cur.execute(sql, params)
            if cur.rowcount:
                self.stdout.write(f"  {etiqueta:<52} {cur.rowcount:>7,}")
                total += cur.rowcount
        self.stdout.write(f"  {'subtotal':<52} {total:>7,}")
        return total
