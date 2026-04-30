from __future__ import annotations

import secrets
import string

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import Sucursal, UserProfile
from logistica.models import Repartidor


class Command(BaseCommand):
    help = "Crea usuarios repartidores exclusivos para la PWA de logística."

    REPARTIDORES = [
        {
            "username": "rep.jose.montoya",
            "first_name": "José Luis",
            "last_name": "Montoya Huitrón",
        },
        {
            "username": "rep.cesar.macias",
            "first_name": "César",
            "last_name": "Macías Hernández",
        },
        {
            "username": "rep.luis.peraza",
            "first_name": "Luis Daniel",
            "last_name": "Peraza Montoya",
        },
        {
            "username": "rep.jorge.perez",
            "first_name": "Jorge Isaac",
            "last_name": "Pérez Valenzuela",
        },
        {
            "username": "rep.jose.galvez",
            "first_name": "José Antonio",
            "last_name": "Gálvez Gálvez",
        },
        {
            "username": "rep.jorge.lopez",
            "first_name": "Jorge Alfonso",
            "last_name": "López Villalobos",
        },
        {
            "username": "rep.everardo.rodriguez",
            "first_name": "Everardo Silvestre",
            "last_name": "Rodríguez Lizárraga",
        },
        {
            "username": "rep.carolina.cayetano",
            "first_name": "Carolina",
            "last_name": "Cayetano Valenzuela",
        },
    ]

    LOCK_FIELDS = [
        "lock_maestros",
        "lock_recetas",
        "lock_compras",
        "lock_inventario",
        "lock_reportes",
        "lock_crm",
        "lock_logistica",
        "lock_rrhh",
        "lock_captura_piso",
        "lock_auditoria",
    ]

    def add_arguments(self, parser):
        parser.add_argument(
            "--reset-passwords",
            action="store_true",
            help="Regenera contraseñas temporales también para usuarios existentes.",
        )
        parser.add_argument(
            "--sucursal-codigo",
            default="MATRIZ",
            help="Código de sucursal para crear el perfil de repartidor. Usa MATRIZ por defecto.",
        )

    def _password(self) -> str:
        alphabet = string.ascii_letters + string.digits
        return "Logi-" + "".join(secrets.choice(alphabet) for _ in range(10))

    def _sucursal(self, codigo: str) -> Sucursal:
        sucursal = Sucursal.objects.filter(codigo=codigo).first()
        if not sucursal:
            sucursal = Sucursal.objects.order_by("id").first()
        if not sucursal:
            raise CommandError("No existe ninguna sucursal para asociar a los repartidores.")
        return sucursal

    @transaction.atomic
    def handle(self, *args, **options):
        User = get_user_model()
        reset_passwords = bool(options["reset_passwords"])
        sucursal = self._sucursal(options["sucursal_codigo"])
        grupo_repartidor, _ = Group.objects.get_or_create(name="repartidor")

        creados = 0
        actualizados = 0
        passwords_emitidos = []

        for row in self.REPARTIDORES:
            user, created = User.objects.get_or_create(
                username=row["username"],
                defaults={
                    "first_name": row["first_name"],
                    "last_name": row["last_name"],
                    "is_active": True,
                    "is_staff": False,
                    "is_superuser": False,
                },
            )
            if created:
                creados += 1
            else:
                actualizados += 1
                user.first_name = row["first_name"]
                user.last_name = row["last_name"]
                user.is_active = True
                user.is_staff = False
                user.is_superuser = False

            if created or reset_passwords:
                password = self._password()
                user.set_password(password)
                passwords_emitidos.append((user.username, row["first_name"], row["last_name"], password))

            user.save()
            user.groups.set([grupo_repartidor])

            profile, _ = UserProfile.objects.get_or_create(user=user)
            profile.sucursal = sucursal
            profile.modo_captura_sucursal = False
            for field in self.LOCK_FIELDS:
                setattr(profile, field, True)
            profile.save()

            Repartidor.objects.update_or_create(
                user=user,
                defaults={
                    "sucursal": sucursal,
                    "unidad_asignada": None,
                },
            )

        self.stdout.write(self.style.SUCCESS(f"Repartidores PWA: {creados} creados, {actualizados} actualizados"))
        if passwords_emitidos:
            self.stdout.write("username,nombre,password_temporal")
            for username, first_name, last_name, password in passwords_emitidos:
                self.stdout.write(f"{username},{first_name} {last_name},{password}")
        else:
            self.stdout.write("No se emitieron contraseñas nuevas. Usa --reset-passwords si necesitas regenerarlas.")
