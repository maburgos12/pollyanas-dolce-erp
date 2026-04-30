from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from rrhh.models import Empleado, NominaConceptoLinea, NominaImportacion, NominaLinea, NominaPeriodo
from rrhh.services.lista_raya import PERCEPCION, parse_lista_raya_xls
from recetas.utils.normalizacion import normalizar_nombre


class Command(BaseCommand):
    help = "Importa una lista de raya de CONTPAQi Nóminas en formato .xls al módulo RRHH."

    def add_arguments(self, parser):
        parser.add_argument("archivo", type=str, help="Ruta al archivo .xls de lista de raya.")
        parser.add_argument(
            "--commit",
            action="store_true",
            help="Guarda empleados, periodo, líneas y conceptos. Sin esta bandera solo valida.",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Si el periodo ya existe, elimina sus líneas y conceptos antes de importar.",
        )

    def handle(self, *args, **options):
        archivo = Path(options["archivo"])
        if not archivo.exists():
            raise CommandError(f"No existe el archivo: {archivo}")

        result = parse_lista_raya_xls(archivo)
        summary = result.validation_summary()
        self._print_summary(result, summary)

        if not all(
            [
                summary["cuadra_empleados"],
                summary["cuadra_percepciones"],
                summary["cuadra_deducciones"],
                summary["cuadra_neto"],
            ]
        ):
            raise CommandError("La lista de raya no cuadra contra los totales generales; no se importó.")

        if not options["commit"]:
            self.stdout.write(self.style.WARNING("Validación OK. Ejecuta con --commit para importar."))
            return

        if not result.fecha_inicio or not result.fecha_fin:
            raise CommandError("No se pudo detectar el rango de fechas del periodo.")

        with transaction.atomic():
            periodo, created = NominaPeriodo.objects.get_or_create(
                fecha_inicio=result.fecha_inicio,
                fecha_fin=result.fecha_fin,
                tipo_periodo=NominaPeriodo.TIPO_QUINCENAL,
                defaults={
                    "estatus": NominaPeriodo.ESTATUS_BORRADOR,
                    "notas": f"Importado desde lista de raya: {archivo.name}",
                },
            )
            if not created and periodo.lineas.exists():
                if not options["replace"]:
                    raise CommandError(
                        f"El periodo {periodo.folio} ya tiene líneas. Usa --replace para reimportar."
                )
                periodo.lineas.all().delete()

            empleados_by_codigo = {
                empleado.codigo: empleado
                for empleado in Empleado.objects.filter(codigo__in=[row.codigo for row in result.empleados])
            }
            empleados_to_create: list[Empleado] = []
            empleados_to_update: list[Empleado] = []
            for row in result.empleados:
                empleado = empleados_by_codigo.get(row.codigo)
                if not empleado:
                    empleado = Empleado(codigo=row.codigo)
                    empleados_to_create.append(empleado)
                else:
                    empleados_to_update.append(empleado)
                empleado.nombre = row.nombre
                empleado.nombre_normalizado = normalizar_nombre(row.nombre)
                empleado.rfc = row.rfc
                empleado.curp = row.curp
                empleado.nss = row.nss
                empleado.area = row.area
                empleado.fecha_ingreso = row.fecha_ingreso or timezone.localdate()
                empleado.salario_diario = row.salario_diario
                empleado.activo = True

            if empleados_to_create:
                Empleado.objects.bulk_create(empleados_to_create)
            if empleados_to_update:
                Empleado.objects.bulk_update(
                    empleados_to_update,
                    ["nombre", "nombre_normalizado", "rfc", "curp", "nss", "area", "fecha_ingreso", "salario_diario", "activo"],
                )

            empleados_by_codigo = {
                empleado.codigo: empleado
                for empleado in Empleado.objects.filter(codigo__in=[row.codigo for row in result.empleados])
            }
            lineas_to_create: list[NominaLinea] = []
            row_by_codigo = {row.codigo: row for row in result.empleados}
            for row in result.empleados:
                empleado = empleados_by_codigo[row.codigo]
                sueldo = _sum_concepts(row.conceptos, PERCEPCION, "Sueldo")
                lineas_to_create.append(
                    NominaLinea(
                        periodo=periodo,
                        empleado=empleado,
                        dias_trabajados=row.dias_pagados,
                        horas_trabajadas=row.horas_trabajadas,
                        horas_dia=row.horas_dia,
                        horas_extra=row.horas_extra,
                        ausencias=row.ausencias,
                        incapacidades=row.incapacidades,
                        sdi=row.sdi,
                        sbc=row.sbc,
                        salario_base=sueldo,
                        bonos=row.total_percepciones - sueldo,
                        descuentos=row.total_deducciones,
                        total_percepciones=row.total_percepciones,
                        neto_calculado=row.neto,
                    )
                )

            NominaLinea.objects.bulk_create(lineas_to_create)
            lineas_by_codigo = {
                linea.empleado.codigo: linea
                for linea in periodo.lineas.select_related("empleado").filter(
                    empleado__codigo__in=row_by_codigo.keys()
                )
            }
            conceptos_to_create: list[NominaConceptoLinea] = []
            for codigo, row in row_by_codigo.items():
                linea = lineas_by_codigo[codigo]
                conceptos_to_create.extend(
                    [
                        NominaConceptoLinea(
                            linea=linea,
                            tipo=concept.tipo,
                            codigo_concepto=concept.codigo,
                            nombre=concept.nombre,
                            valor=concept.valor,
                            importe=concept.importe,
                        )
                        for concept in row.conceptos
                    ]
                )
            NominaConceptoLinea.objects.bulk_create(conceptos_to_create)

            periodo.recompute_totals()
            periodo.save(update_fields=["total_bruto", "total_descuentos", "total_neto", "updated_at"])
            importacion = NominaImportacion.objects.create(
                archivo_nombre=archivo.name,
                archivo_hash=result.source_hash,
                periodo=periodo,
                estatus=NominaImportacion.ESTATUS_IMPORTADA,
                empleados_detectados=len(result.empleados),
                total_percepciones=result.total_percepciones_calculado,
                total_deducciones=result.total_deducciones_calculado,
                total_neto=result.total_neto_calculado,
                resumen=summary,
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Importación {importacion.id} lista: {len(result.empleados)} empleados, periodo {periodo.folio}."
            )
        )

    def _print_summary(self, result, summary):
        self.stdout.write(f"Archivo: {Path(result.source_path).name}")
        self.stdout.write(f"Empresa: {result.empresa}")
        self.stdout.write(f"Periodo: {result.fecha_inicio} a {result.fecha_fin} ({result.periodo_numero})")
        self.stdout.write(f"Empleados: {summary['empleados_detectados']} / {summary['empleados_reportados']}")
        self.stdout.write(
            "Totales: "
            f"percepciones {summary['total_percepciones_calculado']} / {summary['total_percepciones_reportado']}, "
            f"deducciones {summary['total_deducciones_calculado']} / {summary['total_deducciones_reportado']}, "
            f"neto {summary['total_neto_calculado']} / {summary['total_neto_reportado']}"
        )


def _sum_concepts(concepts, tipo: str, nombre: str) -> Decimal:
    return sum(
        (concept.importe for concept in concepts if concept.tipo == tipo and concept.nombre.strip() == nombre),
        Decimal("0"),
    )
