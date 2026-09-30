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

import re

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

# Los activos no los decide una fecha sino a dónde se los llevaron, y eso lo
# confirmó operaciones: los ocho equipos son del área de venta —lo que se
# necesita para vender— y se mudaron completos al local nuevo. El renglón de la
# desinstalación eléctrica no se mueve porque no es un activo: es el gasto de
# desmantelar el local viejo, y ese gasto fue de Crucero.
MEDIDOR_BAMOA = "543220903285"  # No. de servicio CFE de Estación Bamoa

# Bamoa no tenía patrones de texto; sin ellos un CFDI que la nombre no resuelve.
PATRONES_BAMOA = [
    ("SUC BAMOA", 10, "Factura global Bamoa"),
    ("BAMOA", 30, "Referencia Bamoa"),
]

ACTIVOS_A_BAMOA = [
    "ACT-2606-119",  # mesa refrigerada
    "ACT-2606-120",  # aire acondicionado
    "ACT-2606-121",  # vitrina
    "ACT-2606-122",  # refrigerador
    "ACT-2606-123",  # báscula
    "ACT-2606-124",  # computadora
    "ACT-2606-125",  # impresora de punto de venta
    "ACT-2608-003",  # instalaciones generales
]

# Queda fuera a propósito:
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
        "periodo_id IN (SELECT id FROM bonos_ventas_configbonoventasperiodo "
        "WHERE (anio > 2026) OR (anio = 2026 AND mes >= 7))",
    ),
]

# Describen la tienda de hoy, no un periodo: pasan completas a Bamoa.
ESTADO_ACTUAL = [
    ("rrhh_empleado", "sucursal_ref_id", "empleados asignados"),
    ("core_userprofile", "sucursal_id", "perfiles de usuario"),
    ("logistica_puntologistico", "sucursal_id", "punto logístico"),
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
            self._reportar_conflictos(cur, params)
            total += self._activos(cur, bamoa)
            self._fiscal(cur, bamoa)
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
        # Las tablas que sólo se consultan dentro de un subselect también tienen
        # que existir: si no, el error aparece a media migración.
        for _tabla, _col, condicion in POR_PADRE:
            nombradas |= set(re.findall(r"FROM\s+([a-z_]+)", condicion))
        existentes = set(connection.introspection.table_names())
        return sorted(nombradas - existentes)

    @staticmethod
    def _llave_unica(tabla: str, col: str) -> list[str]:
        """Columnas con las que la tabla distingue un renglón, además de la sucursal.

        Point siguió escribiendo con el registro nuevo desde que se repuntó, así
        que el destino ya puede tener el renglón que vamos a mover. Sin esto la
        migración aborta por llave duplicada a media faena.
        """
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname
                FROM pg_constraint c
                JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
                WHERE c.conrelid = %s::regclass AND c.contype = 'u'
                  AND %s = ANY(SELECT att.attname FROM pg_attribute att
                               WHERE att.attrelid = c.conrelid AND att.attnum = ANY(c.conkey))
                """,
                [tabla, col],
            )
            return [fila[0] for fila in cur.fetchall() if fila[0] != col]

    def _sql_fecha(self):
        for tabla, col, fecha, corte in POR_FECHA:
            if tabla not in connection.introspection.table_names():
                continue
            resguardo = ""
            claves = self._llave_unica(tabla, col)
            if claves:
                iguales = " AND ".join(f'o."{k}" = "{tabla}"."{k}"' for k in claves)
                resguardo = (
                    f' AND NOT EXISTS (SELECT 1 FROM "{tabla}" o '
                    f'WHERE o."{col}" = %(destino)s AND {iguales})'
                )
            yield (
                f"{tabla}.{col}",
                f'UPDATE "{tabla}" SET "{col}" = %(destino)s '
                f'WHERE "{col}" = %(origen)s AND "{fecha}" >= \'{corte}\'{resguardo}',
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

    def _fiscal(self, cur, bamoa) -> None:
        """Sólo el medidor de Bamoa es suyo; «CRUCERO» seguiría nombrando al local viejo."""
        self.stdout.write("")
        self.stdout.write("PATRONES FISCALES")
        if "conciliacion_sucursalidentificadorfiscal" not in connection.introspection.table_names():
            self.stdout.write("  tabla no presente, se omite")
            return 0
        cur.execute(
            'UPDATE "conciliacion_sucursalidentificadorfiscal" SET "sucursal_id" = %s '
            'WHERE "patron" = %s',
            [bamoa.pk, MEDIDOR_BAMOA],
        )
        self.stdout.write(f"  medidor CFE de Estación Bamoa → Bamoa{'':<18} {cur.rowcount:>7,}")
        self.stdout.write("  «CRUCERO», «SUC CRUCERO» y el medidor de Niños Héroes se quedan")

        # Sin patrones de texto propios, un CFDI que diga «Bamoa» no resuelve a
        # ninguna sucursal. Se siguen las prioridades del resto del catálogo.
        creados = 0
        for patron, prioridad, descripcion in PATRONES_BAMOA:
            cur.execute(
                'INSERT INTO "conciliacion_sucursalidentificadorfiscal" '
                '("patron","tipo","descripcion","prioridad","activo","sucursal_id","creado_en","actualizado_en") '
                "VALUES (%s,'texto',%s,%s,true,%s,NOW(),NOW()) ON CONFLICT DO NOTHING",
                [patron, descripcion, prioridad, bamoa.pk],
            )
            creados += cur.rowcount
        self.stdout.write(f"  patrones de texto propios de Bamoa dados de alta{'':<3} {creados:>7,}")

    def _reportar_conflictos(self, cur, params) -> None:
        """Lo que se quedó en Crucero porque el destino ya lo tenía.

        No se borra: un renglón que no se pudo mover se enseña para que una
        persona decida, en vez de desaparecer sin que nadie lo note.
        """
        pendientes = []
        for tabla, col, fecha, corte in POR_FECHA:
            if tabla not in connection.introspection.table_names():
                continue
            cur.execute(
                f'SELECT COUNT(*) FROM "{tabla}" WHERE "{col}" = %(origen)s '
                f'AND "{fecha}" >= \'{corte}\'',
                params,
            )
            quedan = cur.fetchone()[0]
            if quedan:
                pendientes.append((tabla, col, quedan))
        if not pendientes:
            return
        self.stdout.write("")
        self.stdout.write(self.style.WARNING("SE QUEDARON EN CRUCERO · el destino ya tenía ese renglón"))
        for tabla, col, quedan in pendientes:
            self.stdout.write(f"  {tabla}.{col:<28} {quedan:>7,}")
        self.stdout.write("  Point los reescribió con el registro nuevo; conviene revisarlos.")

    def _activos(self, cur, bamoa) -> int:
        """A dónde se llevaron los equipos lo sabe operaciones, no una fecha."""
        self.stdout.write("")
        self.stdout.write("ACTIVOS")
        if "activos_activo" not in connection.introspection.table_names():
            self.stdout.write("  tabla no presente, se omite")
            return 0
        cur.execute(
            'UPDATE "activos_activo" SET "sucursal_id" = %s, "ubicacion" = %s '
            'WHERE "codigo" = ANY(%s)',
            [bamoa.pk, bamoa.nombre, ACTIVOS_A_BAMOA],
        )
        movidos = cur.rowcount
        self.stdout.write(f"  {'equipo de venta, al local nuevo':<52} {movidos:>7,}")
        self.stdout.write("  la desinstalación eléctrica se queda: es un gasto de Crucero")
        return movidos

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
