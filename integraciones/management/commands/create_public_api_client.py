from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from integraciones.models import PublicApiClient


class Command(BaseCommand):
    help = "Crea un cliente para API pública y devuelve la API key (se muestra una sola vez)."

    def add_arguments(self, parser):
        parser.add_argument("--nombre", required=True, help="Nombre del cliente integrador")
        parser.add_argument("--descripcion", default="", help="Descripción del cliente")
        parser.add_argument("--username", default="", help="Usuario creador opcional")

    def handle(self, *args, **options):
        nombre = options["nombre"].strip()
        descripcion = options["descripcion"].strip()
        username = options["username"].strip()
        if not nombre:
            raise CommandError("--nombre es obligatorio")

        created_by = None
        if username:
            User = get_user_model()
            created_by = User.objects.filter(username=username).first()
            if not created_by:
                raise CommandError(f"No existe usuario: {username}")

        obj, raw_key = PublicApiClient.create_with_generated_key(
            nombre=nombre,
            descripcion=descripcion,
            created_by=created_by,
        )
        self.stdout.write(self.style.SUCCESS("Cliente API pública creado"))
        self.stdout.write(f"  id: {obj.id}")
        self.stdout.write(f"  nombre: {obj.nombre}")
        self.stdout.write(f"  prefijo: {obj.clave_prefijo}")
        self.stdout.write("  api_key (guardar ahora):")
        self.stdout.write(raw_key)
