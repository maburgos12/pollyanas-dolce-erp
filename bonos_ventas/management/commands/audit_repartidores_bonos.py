"""
Diagnostica y opcionalmente repara el vínculo Repartidor <-> Empleado
necesario para que sync_dias_repartidor funcione.

Uso:
    python manage.py audit_repartidores_bonos           # solo diagnóstico
    python manage.py audit_repartidores_bonos --fix     # intenta reparar automáticamente
"""
from __future__ import annotations

from django.core.management.base import BaseCommand

from logistica.models import Repartidor
from rrhh.models import Empleado


class Command(BaseCommand):
    help = "Audita el vínculo Repartidor(logística) ↔ Empleado(RRHH) para bonos de ventas."

    def add_arguments(self, parser):
        parser.add_argument(
            "--fix",
            action="store_true",
            help="Intenta ligar Empleados existentes al Repartidor vía usuario_erp cuando el User coincide.",
        )

    def handle(self, *args, **options):
        repartidores = Repartidor.objects.select_related("user", "sucursal").all()

        ok, sin_empleado, sin_area, sin_usuario_erp = [], [], [], []

        for r in repartidores:
            user = r.user
            emp = getattr(user, "empleado_rrhh", None)

            if emp is None:
                sin_empleado.append(r)
                if options["fix"]:
                    emp = self._intentar_reparar(r, user)

            if emp is not None:
                if (emp.puesto_operativo or "").strip().upper() != "REPARTIDOR":
                    sin_area.append((r, emp))
                elif emp.usuario_erp_id != user.pk:
                    sin_usuario_erp.append((r, emp))
                else:
                    ok.append(r)

        self.stdout.write(self.style.SUCCESS(f"\n✔  Correctamente vinculados: {len(ok)}"))
        for r in ok:
            self.stdout.write(f"     {r.user.username} → {r.user.empleado_rrhh.nombre}")

        if sin_empleado:
            self.stdout.write(self.style.WARNING(f"\n⚠  Sin Empleado vinculado: {len(sin_empleado)}"))
            self.stdout.write("   Acción requerida: crear un Empleado con área=REPARTIDORES y puesto_operativo=REPARTIDOR")
            self.stdout.write("   y asignar Empleado.usuario_erp = ese User.\n")
            for r in sin_empleado:
                self.stdout.write(
                    f"     user={r.user.username!r}  nombre={r.user.get_full_name()!r}  sucursal={r.sucursal}"
                )

        if sin_area:
            self.stdout.write(self.style.WARNING(f"\n⚠  Empleado vinculado pero puesto_operativo ≠ REPARTIDOR: {len(sin_area)}"))
            for r, emp in sin_area:
                self.stdout.write(f"     {emp.nombre} (puesto_operativo actual: {emp.puesto_operativo!r}) → cambiar a REPARTIDOR")

        if sin_usuario_erp:
            self.stdout.write(self.style.ERROR(f"\n✘  usuario_erp inconsistente: {len(sin_usuario_erp)}"))
            for r, emp in sin_usuario_erp:
                self.stdout.write(f"     Repartidor user={r.user.username} / Empleado usuario_erp={emp.usuario_erp}")

        if not sin_empleado and not sin_area and not sin_usuario_erp:
            self.stdout.write(self.style.SUCCESS("\nTodos los repartidores están correctamente vinculados.\n"))
        else:
            self.stdout.write(
                self.style.NOTICE(
                    "\nPara reparar automáticamente cuando el Empleado ya existe:\n"
                    "  python manage.py audit_repartidores_bonos --fix\n"
                )
            )

    def _intentar_reparar(self, repartidor: Repartidor, user) -> Empleado | None:
        full_name = user.get_full_name().strip()
        if not full_name:
            return None

        candidatos = Empleado.objects.filter(nombre__icontains=full_name, usuario_erp__isnull=True)
        if candidatos.count() == 1:
            emp = candidatos.first()
            emp.usuario_erp = user
            if (emp.puesto_operativo or "").strip().upper() != "REPARTIDOR":
                emp.area = "REPARTIDORES"
                emp.puesto_operativo = "REPARTIDOR"
            emp.save(update_fields=["usuario_erp", "area", "puesto_operativo"])
            self.stdout.write(
                self.style.SUCCESS(
                    f"  [FIX] {user.username} → Empleado '{emp.nombre}' vinculado, area=REPARTIDORES, puesto_operativo=REPARTIDOR"
                )
            )
            return emp
        elif candidatos.count() > 1:
            self.stdout.write(
                self.style.WARNING(
                    f"  [FIX] {user.username}: múltiples candidatos con nombre '{full_name}', se requiere intervención manual."
                )
            )
        return None
