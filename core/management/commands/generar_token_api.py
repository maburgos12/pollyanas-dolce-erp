from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from rest_framework.authtoken.models import Token


class Command(BaseCommand):
    help = "Genera o rota token API para un usuario."

    def add_arguments(self, parser):
        parser.add_argument("--username", required=True, help="Username del usuario")
        parser.add_argument(
            "--rotate",
            action="store_true",
            help="Elimina token previo y crea uno nuevo.",
        )

    def handle(self, *args, **options):
        username = (options.get("username") or "").strip()
        if not username:
            raise CommandError("Debes enviar --username.")

        user_model = get_user_model()
        user = user_model.objects.filter(username=username).first()
        if user is None:
            raise CommandError(f"Usuario no encontrado: {username}")

        if bool(options.get("rotate")):
            Token.objects.filter(user=user).delete()
            token = Token.objects.create(user=user)
            action = "rotated"
        else:
            token, created = Token.objects.get_or_create(user=user)
            action = "created" if created else "existing"

        self.stdout.write(
            self.style.SUCCESS(
                f"TOKEN_READY username={user.username} action={action} token={token.key}"
            )
        )
