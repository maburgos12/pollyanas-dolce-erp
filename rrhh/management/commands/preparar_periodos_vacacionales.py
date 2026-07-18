import calendar
import csv
import os
import tempfile
from collections import Counter
from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count, Q, Sum
from django.utils import timezone

from rrhh.models import (
    AplicacionGoceVacaciones,
    Empleado,
    MovimientoVacaciones,
    PeriodoVacacional,
    SolicitudVacaciones,
)
from rrhh.services_vacaciones import (
    antiguedad_anios,
    dias_generados_para_empleado,
    politica_para_empleado,
)
from rrhh.services_vacaciones_saldos import saldo_periodo_vacacional


CSV_COLUMNS = [
    "empleado_id",
    "empleado",
    "periodo",
    "saldo_actual",
    "saldo_propuesto",
    "diferencia",
    "clasificacion",
    "detalle",
]


def _safe_date(year: int, month: int, day: int) -> date:
    if month == 2 and day == 29 and not calendar.isleap(year):
        day = 28
    return date(year, month, day)


def _add_6_months(d: date) -> date:
    month = d.month + 6
    year = d.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(d.day, last_day))


class Command(BaseCommand):
    help = "Prepara bolsas vacacionales y aplicaciones de goce legacy en orden FIFO."

    def add_arguments(self, parser):
        parser.add_argument("--ejecutar", action="store_true")
        parser.add_argument("--empleado-id", type=int)
        parser.add_argument("--salida-csv")

    def handle(self, *args, **options):
        ejecutar = options["ejecutar"]
        registros = []
        candidatos_por_empleado = {}
        empleados = Empleado.objects.order_by("id")
        if options.get("empleado_id") is not None:
            empleados = empleados.filter(pk=options["empleado_id"])

        conflicto_por_empleado = {}
        with transaction.atomic():
            for empleado in empleados:
                candidatos = {}
                conflicto_hist = self._preparar_historicos(
                    empleado, candidatos, registros, ejecutar=ejecutar
                )
                conflicto_aniv = self._preparar_ultimo_aniversario(
                    empleado, candidatos, registros, ejecutar=ejecutar
                )
                candidatos_por_empleado[empleado.pk] = list(
                    sorted(
                        candidatos.values(),
                        key=lambda periodo: (periodo.aniversario, periodo.pk),
                    )
                )
                conflicto_por_empleado[empleado.pk] = bool(conflicto_hist or conflicto_aniv)

            for empleado in empleados:
                self._aplicar_solicitudes_legacy(
                    empleado,
                    candidatos_por_empleado[empleado.pk],
                    registros,
                    ejecutar=ejecutar,
                    tiene_conflicto=conflicto_por_empleado[empleado.pk],
                )

            if options.get("salida_csv"):
                self._escribir_csv(options["salida_csv"], registros)

            if not ejecutar:
                transaction.set_rollback(True)

        conteos = Counter(registro["clasificacion"] for registro in registros)
        for clasificacion in (
            "PROPUESTA",
            "CREADA",
            "REUTILIZADA",
            "REQUIERE_REVISION",
        ):
            self.stdout.write(f"{clasificacion}: {conteos[clasificacion]}")

    def _registrar(
        self,
        registros,
        empleado,
        *,
        periodo=None,
        periodo_fecha=None,
        saldo_actual=Decimal("0"),
        saldo_propuesto=Decimal("0"),
        clasificacion,
        detalle,
    ):
        fecha = periodo.aniversario if periodo is not None else periodo_fecha
        registros.append(
            {
                "empleado_id": empleado.pk,
                "empleado": empleado.nombre,
                "periodo": fecha.isoformat() if fecha else "",
                "saldo_actual": saldo_actual,
                "saldo_propuesto": saldo_propuesto,
                "diferencia": saldo_propuesto - saldo_actual,
                "clasificacion": clasificacion,
                "detalle": detalle,
            }
        )

    def _preparar_historicos(self, empleado, candidatos, registros, *, ejecutar) -> bool:
        conflicto = False
        historicos = (
            MovimientoVacaciones.objects.filter(
                empleado=empleado,
                tipo=MovimientoVacaciones.TIPO_AJUSTE,
                descripcion__icontains="pendiente de goce",
            )
            .values("periodo_anio")
            .annotate(
                total_dias=Sum("dias"),
                invalidos=Count("pk", filter=Q(dias__lte=0)),
            )
            .order_by("periodo_anio")
        )
        for historico in historicos:
            periodo_anio = historico["periodo_anio"]
            total_dias = historico["total_dias"] or Decimal("0")
            invalidos = historico["invalidos"] or 0
            aniversario = _safe_date(
                periodo_anio, empleado.fecha_ingreso.month, empleado.fecha_ingreso.day
            )
            if total_dias <= 0 or invalidos > 0:
                conflicto = True
                self._registrar(
                    registros,
                    empleado,
                    periodo_fecha=aniversario,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        "El saldo histórico pendiente de goce tiene registros "
                        "individuales con días ≤ 0 o suma no positiva."
                    ),
                )
                continue
            periodo = PeriodoVacacional.objects.filter(
                empleado=empleado, aniversario=aniversario
            ).first()
            if periodo is None:
                periodo = PeriodoVacacional.objects.create(
                    empleado=empleado,
                    aniversario=aniversario,
                    fecha_limite=_add_6_months(aniversario),
                    antiguedad_anios=antiguedad_anios(empleado, al=aniversario),
                    dias_generados=total_dias,
                    origen="saldo_inicial",
                )
                candidatos[aniversario] = periodo
                self._registrar(
                    registros,
                    empleado,
                    periodo=periodo,
                    saldo_propuesto=total_dias,
                    clasificacion="CREADA" if ejecutar else "PROPUESTA",
                    detalle="Bolsa histórica creada desde ajustes pendientes de goce.",
                )
            elif (
                periodo.dias_generados == total_dias
                and periodo.origen == "saldo_inicial"
                and periodo.fecha_limite == _add_6_months(aniversario)
                and periodo.antiguedad_anios == antiguedad_anios(empleado, al=aniversario)
            ):
                candidatos[aniversario] = periodo
                saldo = saldo_periodo_vacacional(periodo).disponible_goce
                self._registrar(
                    registros,
                    empleado,
                    periodo=periodo,
                    saldo_actual=saldo,
                    saldo_propuesto=saldo,
                    clasificacion="REUTILIZADA",
                    detalle=(
                        "La bolsa histórica existente coincide con el origen y "
                        "saldo esperados."
                    ),
                )
            else:
                conflicto = True
                saldo = saldo_periodo_vacacional(periodo).disponible_goce
                self._registrar(
                    registros,
                    empleado,
                    periodo=periodo,
                    saldo_actual=saldo,
                    saldo_propuesto=saldo,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        "La bolsa histórica existente tiene origen, días, "
                        "fecha_limite o antigüedad distintos; no se alteró."
                    ),
                )
        return conflicto

    def _preparar_ultimo_aniversario(self, empleado, candidatos, registros, *, ejecutar) -> bool:
        hoy = timezone.localdate()
        ultimo_aniv = _safe_date(
            hoy.year, empleado.fecha_ingreso.month, empleado.fecha_ingreso.day
        )
        if ultimo_aniv > hoy:
            ultimo_aniv = _safe_date(
                hoy.year - 1, empleado.fecha_ingreso.month, empleado.fecha_ingreso.day
            )
        antiguedad = antiguedad_anios(empleado, al=ultimo_aniv)
        if antiguedad < 1:
            self._registrar(
                registros,
                empleado,
                periodo_fecha=ultimo_aniv,
                clasificacion="REQUIERE_REVISION",
                detalle="El último aniversario no completa un año de antigüedad.",
            )
            return True
        if politica_para_empleado(empleado, al=ultimo_aniv) is None:
            self._registrar(
                registros,
                empleado,
                periodo_fecha=ultimo_aniv,
                clasificacion="REQUIERE_REVISION",
                detalle="No existe política vacacional aplicable al aniversario.",
            )
            return True

        dias_calculados = dias_generados_para_empleado(empleado, al=ultimo_aniv)
        periodo = PeriodoVacacional.objects.filter(
            empleado=empleado, aniversario=ultimo_aniv
        ).first()
        if periodo is None:
            periodo = PeriodoVacacional.objects.create(
                empleado=empleado,
                aniversario=ultimo_aniv,
                fecha_limite=_add_6_months(ultimo_aniv),
                antiguedad_anios=antiguedad,
                dias_generados=dias_calculados,
                origen="calculado",
            )
            candidatos[ultimo_aniv] = periodo
            self._registrar(
                registros,
                empleado,
                periodo=periodo,
                saldo_propuesto=dias_calculados,
                clasificacion="CREADA" if ejecutar else "PROPUESTA",
                detalle="Bolsa calculada para el último aniversario.",
            )
            return False
        elif (
            periodo.dias_generados == dias_calculados
            and periodo.origen == "calculado"
            and periodo.fecha_limite == _add_6_months(ultimo_aniv)
            and periodo.antiguedad_anios == antiguedad
        ):
            candidatos[ultimo_aniv] = periodo
            saldo = saldo_periodo_vacacional(periodo).disponible_goce
            self._registrar(
                registros,
                empleado,
                periodo=periodo,
                saldo_actual=saldo,
                saldo_propuesto=saldo,
                clasificacion="REUTILIZADA",
                detalle="La bolsa calculada existente coincide con la política aplicable.",
            )
            return False
        else:
            saldo = saldo_periodo_vacacional(periodo).disponible_goce
            self._registrar(
                registros,
                empleado,
                periodo=periodo,
                saldo_actual=saldo,
                saldo_propuesto=saldo,
                clasificacion="REQUIERE_REVISION",
                detalle=(
                    "El aniversario ya tiene una bolsa con origen, días, "
                    "fecha_limite o antigüedad distintos; no se alteró."
                ),
            )
            return True

    def _aplicar_solicitudes_legacy(
        self,
        empleado,
        periodos,
        registros,
        *,
        ejecutar,
        tiene_conflicto=False,
    ):
        solicitudes = list(
            SolicitudVacaciones.objects.filter(
                empleado=empleado,
                estado=SolicitudVacaciones.ESTADO_APROBADA,
            )
            .annotate(total_aplicaciones=Count("aplicaciones_goce"))
            .order_by("fecha_aprobacion_rrhh", "creado_en", "id")
        )
        # Fix #3: bolsa con conflicto bloquea TODAS las aplicaciones legacy del empleado
        if tiene_conflicto:
            for solicitud in solicitudes:
                self._registrar(
                    registros,
                    empleado,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        f"La solicitud {solicitud.folio} no se procesó: "
                        "el empleado tiene bolsas con conflicto."
                    ),
                )
            return

        restaurado_por_periodo = {
            fila["periodo_id"]: fila["total"]
            for fila in AplicacionGoceVacaciones.objects.filter(
                solicitud__in=solicitudes,
                periodo__in=periodos,
                estado=AplicacionGoceVacaciones.ESTADO_CONSUMIDA,
            )
            .values("periodo_id")
            .annotate(total=Sum("dias"))
        }
        saldos_virtuales = {
            periodo.pk: (
                saldo_periodo_vacacional(periodo).disponible_goce
                + restaurado_por_periodo.get(periodo.pk, Decimal("0"))
            )
            for periodo in periodos
        }
        linea_temporal_ambigua = False
        aplicaciones_planeadas = []

        for solicitud in solicitudes:
            if linea_temporal_ambigua:
                self._registrar(
                    registros,
                    empleado,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        f"La solicitud {solicitud.folio} no se procesa porque una "
                        "solicitud anterior dejó la línea FIFO ambigua."
                    ),
                )
                continue

            # Fix 2a: solicitud sin días laborables válidos
            if solicitud.dias_laborables <= 0:
                self._registrar(
                    registros,
                    empleado,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        f"La solicitud {solicitud.folio} tiene dias_laborables "
                        f"{solicitud.dias_laborables} ≤ 0; no se procesa."
                    ),
                )
                linea_temporal_ambigua = True
                continue

            consumidos = list(
                MovimientoVacaciones.objects.filter(
                    solicitud=solicitud,
                    tipo=MovimientoVacaciones.TIPO_CONSUMIDO,
                ).order_by("creado_en", "id")
            )
            if len(consumidos) != 1:
                self._registrar(
                    registros,
                    empleado,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        f"La solicitud {solicitud.folio} tiene {len(consumidos)} "
                        "movimientos consumidos; se requiere exactamente uno."
                    ),
                )
                linea_temporal_ambigua = True
                continue
            movimiento = consumidos[0]
            # Fix #3: el movimiento consumido debe pertenecer al mismo empleado que la solicitud
            if movimiento.empleado_id != solicitud.empleado_id:
                self._registrar(
                    registros,
                    empleado,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        f"La solicitud {solicitud.folio} tiene un movimiento consumido "
                        f"que pertenece al empleado {movimiento.empleado_id}, "
                        f"no al empleado {solicitud.empleado_id}."
                    ),
                )
                linea_temporal_ambigua = True
                continue
            # Fix 2b: el movimiento consumido debe tener días positivos
            if movimiento.dias <= 0:
                self._registrar(
                    registros,
                    empleado,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        f"La solicitud {solicitud.folio} tiene un movimiento consumido "
                        f"de {movimiento.dias} días ≤ 0; no se procesa."
                    ),
                )
                linea_temporal_ambigua = True
                continue
            # Fix #2: el movimiento consumido debe coincidir exactamente con dias_laborables
            if movimiento.dias != solicitud.dias_laborables:
                self._registrar(
                    registros,
                    empleado,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        f"La solicitud {solicitud.folio} tiene un movimiento consumido "
                        f"de {movimiento.dias} días pero la solicitud registra "
                        f"{solicitud.dias_laborables} días; se requiere igualdad exacta."
                    ),
                )
                linea_temporal_ambigua = True
                continue
            candidatos = []
            cronologia_invalida = False
            for periodo in periodos:
                valido = True
                if periodo.origen == "saldo_inicial":
                    baseline = (
                        MovimientoVacaciones.objects.filter(
                            empleado=empleado,
                            tipo=MovimientoVacaciones.TIPO_AJUSTE,
                            periodo_anio=periodo.aniversario.year,
                            descripcion__icontains="pendiente de goce",
                        )
                        .order_by("-creado_en", "-id")
                        .first()
                    )
                    if not baseline:
                        valido = False
                    else:
                        corte = baseline.creado_en.date()
                        valido = (
                            movimiento.creado_en > baseline.creado_en
                            and solicitud.fecha_inicio > corte
                            and (
                                solicitud.fecha_aprobacion_rrhh is None
                                or solicitud.fecha_aprobacion_rrhh.date() > corte
                            )
                        )
                elif periodo.origen == "calculado":
                    corte = periodo.aniversario
                    valido = (
                        movimiento.creado_en.date() > corte
                        and solicitud.fecha_inicio > corte
                        and (
                            solicitud.fecha_aprobacion_rrhh is None
                            or solicitud.fecha_aprobacion_rrhh.date() > corte
                        )
                    )
                if valido:
                    candidatos.append(periodo)
                else:
                    cronologia_invalida = True
                    saldo = saldo_periodo_vacacional(periodo).disponible_goce
                    self._registrar(
                        registros,
                        empleado,
                        periodo=periodo,
                        saldo_actual=saldo,
                        saldo_propuesto=saldo,
                        clasificacion="REQUIERE_REVISION",
                        detalle=(
                            f"La cronología de la solicitud {solicitud.folio} "
                            "no es compatible con esta bolsa."
                        ),
                    )

            # Fix #1: si CUALQUIER bolsa falla cronología, cero aplicaciones para la solicitud
            if cronologia_invalida:
                self._registrar(
                    registros,
                    empleado,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        f"La solicitud {solicitud.folio} tiene bolsas con cronología "
                        "inválida; se omiten todas las aplicaciones de esta solicitud."
                    ),
                )
                linea_temporal_ambigua = True
                continue

            if not candidatos:
                self._registrar(
                    registros,
                    empleado,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        f"La solicitud {solicitud.folio} no tiene una bolsa "
                        "temporalmente válida."
                    ),
                )
                linea_temporal_ambigua = True
                continue

            apps = []
            apps_por_periodo = {}
            if solicitud.total_aplicaciones > 0:
                apps = list(
                    solicitud.aplicaciones_goce.select_related("periodo").order_by(
                        "periodo__aniversario", "id"
                    )
                )
                aplicaciones_validas = all(
                    app.estado == AplicacionGoceVacaciones.ESTADO_CONSUMIDA
                    and app.dias > 0
                    and app.periodo.empleado_id == solicitud.empleado_id
                    for app in apps
                )
                if not aplicaciones_validas:
                    self._registrar(
                        registros,
                        empleado,
                        clasificacion="REQUIERE_REVISION",
                        detalle=(
                            f"La solicitud {solicitud.folio} ya tiene aplicaciones "
                            "no consumidas o inconsistentes; no se reprocesa."
                        ),
                    )
                    linea_temporal_ambigua = True
                    continue
                apps_por_periodo = {app.periodo_id: app.dias for app in apps}

            pendiente = solicitud.dias_laborables
            distribucion_esperada = {}
            asignaciones = []
            for periodo in candidatos:
                capacidad = saldos_virtuales[periodo.pk]
                aplicado = min(capacidad, pendiente)
                if aplicado > 0:
                    distribucion_esperada[periodo.pk] = aplicado
                    asignaciones.append((periodo, capacidad, aplicado))
                    pendiente -= aplicado
                if pendiente == 0:
                    break

            if pendiente > 0:
                disponible_total = solicitud.dias_laborables - pendiente
                self._registrar(
                    registros,
                    empleado,
                    saldo_actual=disponible_total,
                    saldo_propuesto=disponible_total,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        "Saldo insuficiente para aplicar completa la solicitud "
                        f"{solicitud.folio}."
                    ),
                )
                linea_temporal_ambigua = True
                continue

            if apps:
                if apps_por_periodo == distribucion_esperada:
                    self._registrar(
                        registros,
                        empleado,
                        clasificacion="REUTILIZADA",
                        detalle=(
                            f"La solicitud {solicitud.folio} ya tiene aplicaciones "
                            "consumidas que coinciden exactamente con FIFO."
                        ),
                    )
                    for periodo_id, dias in distribucion_esperada.items():
                        saldos_virtuales[periodo_id] -= dias
                else:
                    self._registrar(
                        registros,
                        empleado,
                        clasificacion="REQUIERE_REVISION",
                        detalle=(
                            f"La solicitud {solicitud.folio} ya tiene aplicaciones "
                            "consumidas que no coinciden con FIFO; no se reprocesa."
                        ),
                    )
                    linea_temporal_ambigua = True
                continue

            aplicaciones_planeadas.append((solicitud, asignaciones))
            for periodo, _saldo_antes, aplicado in asignaciones:
                saldos_virtuales[periodo.pk] -= aplicado

        if linea_temporal_ambigua:
            for solicitud, _asignaciones in aplicaciones_planeadas:
                self._registrar(
                    registros,
                    empleado,
                    clasificacion="REQUIERE_REVISION",
                    detalle=(
                        f"La solicitud {solicitud.folio} era asignable, pero no se "
                        "escribe porque la línea FIFO del empleado quedó ambigua."
                    ),
                )
            return

        for solicitud, asignaciones in aplicaciones_planeadas:
            for periodo, saldo_antes, aplicado in asignaciones:
                AplicacionGoceVacaciones.objects.create(
                    solicitud=solicitud,
                    periodo=periodo,
                    dias=aplicado,
                    estado=AplicacionGoceVacaciones.ESTADO_CONSUMIDA,
                    actor=solicitud.aprobado_rrhh_por,
                )
                self._registrar(
                    registros,
                    empleado,
                    periodo=periodo,
                    saldo_actual=saldo_antes,
                    saldo_propuesto=saldo_antes - aplicado,
                    clasificacion="CREADA" if ejecutar else "PROPUESTA",
                    detalle=f"Aplicación FIFO consumida para solicitud {solicitud.folio}.",
                )

    def _escribir_csv(self, ruta, registros):
        ruta_absoluta = os.path.abspath(ruta)
        directorio = os.path.dirname(ruta_absoluta)
        descriptor, ruta_temporal = tempfile.mkstemp(
            prefix=".vacaciones-sombra-", suffix=".tmp", dir=directorio
        )
        try:
            with os.fdopen(
                descriptor, "w", newline="", encoding="utf-8"
            ) as archivo:
                writer = csv.DictWriter(archivo, fieldnames=CSV_COLUMNS)
                writer.writeheader()
                writer.writerows(registros)
            os.replace(ruta_temporal, ruta_absoluta)
        except Exception:
            if os.path.exists(ruta_temporal):
                os.unlink(ruta_temporal)
            raise
