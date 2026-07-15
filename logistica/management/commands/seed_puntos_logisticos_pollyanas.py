from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from core.models import Sucursal
from logistica.models import PuntoLogistico


class Command(BaseCommand):
    help = "Carga sucursales Pollyana's Dolce como puntos logisticos con geocerca."

    RADIO_SUCURSAL_METROS = 120
    SUCURSALES = [
        {
            "codigo": "MATRIZ",
            "nombre": "Sucursal Matriz",
            "direccion": "Av. Benigno Valenzuela 101, Col. Centro, Guasave, Sinaloa",
            "latitud": "25.567916",
            "longitud": "-108.459969",
            "google_maps_url": "https://maps.app.goo.gl/Qaj865fNmhK337fR9",
        },
        {
            "codigo": "PAYAN",
            "nombre": "Sucursal Payan",
            "direccion": "Blvd. R. Romualdo Ruiz Payan 184-L2, Col. Del Bosque, Guasave, Sinaloa",
            "latitud": "25.568887",
            "longitud": "-108.474745",
            "google_maps_url": "https://maps.app.goo.gl/cYEw3cTmRV9EoPe16",
        },
        {
            "codigo": "LAS_GLORIAS",
            "nombre": "Sucursal Plaza Las Glorias",
            "direccion": "Francisco Gonzalez Bocanegra 87, Plaza Las Glorias, Local 4, Col. Las Huertas, Guasave, Sinaloa",
            "latitud": "25.558665",
            "longitud": "-108.470713",
            "google_maps_url": "https://maps.app.goo.gl/azxPRc3FTf6dFjZj9",
        },
        {
            "codigo": "PLAZA_NIO",
            "nombre": "Sucursal Plaza Nio",
            "direccion": "Paseo Miguel Leyson Perez, Plaza Nio 133, Local 9, Guasave, Sinaloa",
            "latitud": "25.581116",
            "longitud": "-108.474669",
            "google_maps_url": "https://maps.app.goo.gl/JV7ub6XcjY5qmQyZ7",
        },
        {
            "codigo": "LEYVA",
            "nombre": "Sucursal Leyva",
            "direccion": "Calle Jose Maria Pino Suarez esquina con Guadalupe Victoria, Gabriel Leyva Solano, Guasave, Sinaloa",
            "latitud": "25.662940",
            "longitud": "-108.638856",
            "google_maps_url": "https://maps.app.goo.gl/SfEeMtW6yNZAYBoR8",
        },
        {
            "codigo": "COLOSIO",
            "nombre": "Sucursal Colosio",
            "direccion": "Luis Donaldo Colosio 81, 2 de Octubre, La Florida, Guasave, Sinaloa",
            "latitud": "25.586597",
            "longitud": "-108.462676",
            "google_maps_url": "https://maps.app.goo.gl/L7xThrqK36PTYeVx6",
        },
        {
            "codigo": "EL_TUNEL",
            "nombre": "Sucursal El Tunel",
            "direccion": "Juan San Millan S/N, Las Palmillas, Guasave, Sinaloa",
            "latitud": "25.565774",
            "longitud": "-108.477033",
            "google_maps_url": "https://maps.app.goo.gl/YWiCB6Vs1VR3KiXZ9",
        },
        {
            "codigo": "CRUCERO",
            "nombre": "Sucursal Bamoa",
            "direccion": "Bamoa, Guasave, Sinaloa",
            "latitud": "25.702448",
            "longitud": "-108.313204",
            "google_maps_url": "https://maps.app.goo.gl/QY5wRXx5rc1j4Xq39",
        },
        {
            "codigo": "GUAMUCHIL",
            "nombre": "Sucursal Guamuchil",
            "direccion": "Blvd. Rosales 627, Centro, CP 81400, Guamuchil, Sinaloa",
            "latitud": "25.459507",
            "longitud": "-108.087299",
            "google_maps_url": "https://www.google.com/maps/search/?api=1&query=25.4595065,-108.0872993",
        },
    ]

    @transaction.atomic
    def handle(self, *args, **options):
        sucursales_creadas = 0
        sucursales_actualizadas = 0
        puntos_creados = 0
        puntos_actualizados = 0

        for row in self.SUCURSALES:
            sucursal, created = Sucursal.objects.update_or_create(
                codigo=row["codigo"],
                defaults={
                    "nombre": row["nombre"],
                    "activa": True,
                },
            )
            if created:
                sucursales_creadas += 1
            else:
                sucursales_actualizadas += 1

            notas = f"{row['direccion']}\nGoogle Maps: {row['google_maps_url']}"
            _, punto_created = PuntoLogistico.objects.update_or_create(
                sucursal=sucursal,
                tipo=PuntoLogistico.TIPO_SUCURSAL,
                defaults={
                    "nombre": row["nombre"],
                    "latitud": row["latitud"],
                    "longitud": row["longitud"],
                    "radio_geocerca_metros": self.RADIO_SUCURSAL_METROS,
                    "activo": True,
                    "notas": notas,
                },
            )
            if punto_created:
                puntos_creados += 1
            else:
                puntos_actualizados += 1

        self.stdout.write(
            self.style.SUCCESS(
                "Puntos logisticos Pollyana's: "
                f"{sucursales_creadas} sucursales creadas, "
                f"{sucursales_actualizadas} actualizadas, "
                f"{puntos_creados} puntos creados, "
                f"{puntos_actualizados} actualizados"
            )
        )
