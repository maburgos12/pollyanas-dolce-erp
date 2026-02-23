from django.core.management.base import BaseCommand, CommandError

from integraciones.models import PublicApiClient


class Command(BaseCommand):
    help = "Rota la API key de un cliente público existente y muestra la nueva clave (una sola vez)."

    def add_arguments(self, parser):
        parser.add_argument("--id", type=int, default=0, help="ID del cliente API pública")
        parser.add_argument("--prefijo", default="", help="Prefijo de la API key (clave_prefijo)")

    def handle(self, *args, **options):
        client_id = int(options["id"] or 0)
        prefijo = (options["prefijo"] or "").strip()

        if client_id <= 0 and not prefijo:
            raise CommandError("Debes enviar --id o --prefijo")

        qs = PublicApiClient.objects.all()
        if client_id > 0:
            qs = qs.filter(id=client_id)
        if prefijo:
            qs = qs.filter(clave_prefijo=prefijo)
        client = qs.first()
        if not client:
            raise CommandError("No se encontró cliente para rotación")

        new_key = client.rotate_key()
        self.stdout.write(self.style.SUCCESS("API key rotada"))
        self.stdout.write(f"  id: {client.id}")
        self.stdout.write(f"  nombre: {client.nombre}")
        self.stdout.write(f"  nuevo_prefijo: {client.clave_prefijo}")
        self.stdout.write("  api_key (guardar ahora):")
        self.stdout.write(new_key)
