from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from logistica.models import ServicioRealizadoUnidad


class Command(BaseCommand):
    help = "Anula un servicio duplicado de unidad conservando su trazabilidad e importe original."

    def add_arguments(self, parser):
        parser.add_argument("--servicio-id", type=int, required=True)
        parser.add_argument("--duplicado-de", type=int, required=True)
        parser.add_argument("--actor-username", required=True)
        parser.add_argument("--motivo", required=True)
        parser.add_argument("--apply", action="store_true", dest="apply")

    @transaction.atomic
    def handle(self, *args, **options):
        servicio = ServicioRealizadoUnidad.objects.select_for_update().filter(pk=options["servicio_id"]).first()
        valido = ServicioRealizadoUnidad.objects.select_for_update().filter(pk=options["duplicado_de"]).first()
        actor = get_user_model().objects.filter(username=options["actor_username"], is_active=True).first()
        motivo = options["motivo"].strip()

        if servicio is None or valido is None:
            raise CommandError("No existe el servicio origen o el servicio válido indicado.")
        if servicio.pk == valido.pk:
            raise CommandError("Un servicio no puede ser duplicado de sí mismo.")
        if servicio.unidad_id != valido.unidad_id:
            raise CommandError("Los servicios deben pertenecer a la misma unidad.")
        if actor is None:
            raise CommandError("El usuario autorizador no existe o está inactivo.")
        if not motivo:
            raise CommandError("El motivo de anulación es obligatorio.")
        if servicio.anulado_en:
            if servicio.duplicado_de_id == valido.pk and servicio.anulado_por_id == actor.pk:
                self.stdout.write(self.style.WARNING(f"El servicio #{servicio.pk} ya estaba anulado con esta relación."))
                return
            raise CommandError("El servicio ya está anulado con una relación diferente.")

        resumen = (
            f"Servicio #{servicio.pk} ({servicio.unidad.codigo}, ${servicio.costo}) será anulado "
            f"como duplicado de #{valido.pk} por {actor.username}."
        )
        if not options["apply"]:
            self.stdout.write(self.style.WARNING(f"SIMULACIÓN: {resumen} Usa --apply para confirmar."))
            transaction.set_rollback(True)
            return

        servicio.anulado_en = timezone.now()
        servicio.anulado_por = actor
        servicio.motivo_anulacion = motivo
        servicio.duplicado_de = valido
        servicio.save(update_fields=["anulado_en", "anulado_por", "motivo_anulacion", "duplicado_de"])
        self.stdout.write(self.style.SUCCESS(resumen))
