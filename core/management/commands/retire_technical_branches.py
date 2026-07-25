from __future__ import annotations

from dataclasses import dataclass

from django.core.management.base import BaseCommand
from django.db import transaction

from core.models import Sucursal, UserProfile
from logistica.models import PuntoLogistico, Unidad
from pos_bridge.models.branch import PointBranch


TECHNICAL_BRANCH_CODES = ("CODLOG", "DBGX", "DEMO-CEDIS", "DEMO-POINT")


@dataclass
class BranchRetirementSummary:
    code: str
    branch_id: int
    branch_name: str
    active_before: bool
    userprofiles_cleared: int
    puntos_disabled: int
    unidades_disabled: int
    point_branches_disabled: int


class Command(BaseCommand):
    help = "Retira sucursales técnicas/demo del catálogo operativo sin tocar históricos."

    def add_arguments(self, parser):
        parser.add_argument(
            "--execute",
            action="store_true",
            help="Aplica los cambios. Sin esta bandera solo reporta.",
        )

    def handle(self, *args, **options):
        execute = bool(options["execute"])
        branches = list(Sucursal.objects.filter(codigo__in=TECHNICAL_BRANCH_CODES).order_by("codigo"))
        if not branches:
            self.stdout.write("No se encontraron sucursales técnicas.")
            return

        summaries = [self._retire_branch(branch=branch, execute=execute) for branch in branches]
        mode_label = "APLICADO" if execute else "DRY-RUN"
        self.stdout.write(self.style.SUCCESS(f"{mode_label}: retiro de sucursales técnicas"))
        for summary in summaries:
            self.stdout.write(
                f"- {summary.code} ({summary.branch_id}) {summary.branch_name}: "
                f"activa={summary.active_before} -> False, "
                f"userprofiles={summary.userprofiles_cleared}, "
                f"puntos={summary.puntos_disabled}, "
                f"unidades={summary.unidades_disabled}, "
                f"point_branches={summary.point_branches_disabled}"
            )

    @transaction.atomic
    def _retire_branch(self, *, branch: Sucursal, execute: bool) -> BranchRetirementSummary:
        userprofiles_qs = UserProfile.objects.filter(sucursal=branch)
        puntos_qs = PuntoLogistico.objects.filter(sucursal=branch, activo=True)
        unidades_qs = Unidad.objects.filter(sucursal=branch, activa=True)
        point_branches_qs = PointBranch.objects.filter(erp_branch=branch).exclude(status=PointBranch.STATUS_INACTIVE)

        summary = BranchRetirementSummary(
            code=branch.codigo,
            branch_id=branch.id,
            branch_name=branch.nombre,
            active_before=branch.activa,
            userprofiles_cleared=userprofiles_qs.count(),
            puntos_disabled=puntos_qs.count(),
            unidades_disabled=unidades_qs.count(),
            point_branches_disabled=point_branches_qs.count(),
        )

        if not execute:
            transaction.set_rollback(True)
            return summary

        userprofiles_qs.update(sucursal=None)
        puntos_qs.update(activo=False)
        unidades_qs.update(activa=False)
        point_branches_qs.update(status=PointBranch.STATUS_INACTIVE, erp_branch=None)
        if branch.activa:
            branch.activa = False
            branch.save(update_fields=["activa"])
        return summary
