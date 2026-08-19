from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from logistica.models import ServicioRealizadoUnidad, TipoServicioUnidad


class Command(BaseCommand):
    help = "Corrige, con guardas e idempotencia, los servicios #18 y #20 de la Cheyenne GS-CH1."

    def add_arguments(self, parser):
        parser.add_argument("--actor-username", required=True)
        parser.add_argument("--servicio-futuro-id", type=int, default=18)
        parser.add_argument("--servicio-suspension-id", type=int, default=20)
        parser.add_argument("--apply", action="store_true", dest="apply")

    @transaction.atomic
    def handle(self, *args, **options):
        actor = get_user_model().objects.filter(username=options["actor_username"], is_active=True).first()
        futuro = ServicioRealizadoUnidad.objects.select_for_update().select_related("unidad").filter(
            pk=options["servicio_futuro_id"]
        ).first()
        suspension = ServicioRealizadoUnidad.objects.select_for_update().select_related("unidad").filter(
            pk=options["servicio_suspension_id"]
        ).first()
        tipo_correcto = TipoServicioUnidad.objects.filter(nombre="Reparación correctiva", activo=True)

        if actor is None:
            raise CommandError("El usuario autorizador no existe o está inactivo.")
        if futuro is None or suspension is None:
            raise CommandError("No existen los dos servicios indicados.")
        if futuro.unidad.codigo != "GS-CH1" or suspension.unidad.codigo != "GS-CH1":
            raise CommandError("Los dos servicios deben pertenecer exactamente a GS-CH1.")
        if futuro.fecha_servicio != date(2026, 10, 28) or (futuro.costo or Decimal("0")) != Decimal("0"):
            raise CommandError("El servicio futuro no coincide con fecha 2026-10-28 y costo cero.")
        if suspension.costo != Decimal("6898.00") or not suspension.archivo_factura:
            raise CommandError("El servicio de suspensión no conserva costo $6,898.00 y factura.")
        if tipo_correcto.count() != 1:
            raise CommandError("Debe existir un único tipo activo llamado Reparación correctiva.")
        tipo_correcto = tipo_correcto.get()

        if futuro.anulado_en and futuro.motivo_anulacion != "Fecha futura y servicio no confirmado":
            raise CommandError("El servicio futuro ya está anulado con otro motivo.")

        if not options["apply"]:
            self.stdout.write(self.style.WARNING("SIMULACIÓN: se anulará #18 y se reclasificará #20. Usa --apply para confirmar."))
            transaction.set_rollback(True)
            return

        if not futuro.anulado_en:
            futuro.anulado_en = timezone.now()
            futuro.anulado_por = actor
            futuro.motivo_anulacion = "Fecha futura y servicio no confirmado"
            futuro.duplicado_de = None
            futuro.save(update_fields=["anulado_en", "anulado_por", "motivo_anulacion", "duplicado_de"])
        if suspension.tipo_servicio_id != tipo_correcto.pk:
            ServicioRealizadoUnidad.objects.filter(pk=suspension.pk).update(tipo_servicio=tipo_correcto)

        self.stdout.write(self.style.SUCCESS("Cheyenne GS-CH1 corregida; importes y factura fueron preservados."))
