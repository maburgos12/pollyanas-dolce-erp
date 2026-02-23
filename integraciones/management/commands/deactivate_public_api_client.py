from django.core.management.base import BaseCommand, CommandError

from integraciones.models import PublicApiClient


class Command(BaseCommand):
    help = "Activa o desactiva un cliente de API pública."

    def add_arguments(self, parser):
        parser.add_argument("--id", type=int, default=0, help="ID del cliente API pública")
        parser.add_argument("--prefijo", default="", help="Prefijo de la API key (clave_prefijo)")
        parser.add_argument("--activar", action="store_true", help="Activar cliente (default: desactivar)")

    def handle(self, *args, **options):
        client_id = int(options["id"] or 0)
        prefijo = (options["prefijo"] or "").strip()
        activate = bool(options["activar"])

        if client_id <= 0 and not prefijo:
            raise CommandError("Debes enviar --id o --prefijo")

        qs = PublicApiClient.objects.all()
        if client_id > 0:
            qs = qs.filter(id=client_id)
        if prefijo:
            qs = qs.filter(clave_prefijo=prefijo)
        client = qs.first()
        if not client:
            raise CommandError("No se encontró cliente")

        client.activo = activate
        client.save(update_fields=["activo", "updated_at"])
        status_label = "ACTIVO" if client.activo else "INACTIVO"
        self.stdout.write(self.style.SUCCESS("Estado actualizado"))
        self.stdout.write(f"  id: {client.id}")
        self.stdout.write(f"  nombre: {client.nombre}")
        self.stdout.write(f"  estatus: {status_label}")
