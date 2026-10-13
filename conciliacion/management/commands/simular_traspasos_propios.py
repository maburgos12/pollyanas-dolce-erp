from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from conciliacion.services.importador import (
    _cuenta_destino_en_descripcion,
    _numero_cuenta_normalizado,
)
from syncfy_client.models import CuentaBancaria, MovimientoBancario


class Command(BaseCommand):
    help = "Simula la deteccion de traspasos entre cuentas propias sin modificar datos."

    def add_arguments(self, parser):
        parser.add_argument(
            "--banco",
            default=CuentaBancaria.BANCO_BANBAJIO,
            choices=[value for value, _label in CuentaBancaria.BANCO_CHOICES],
        )

    def handle(self, *args, **options):
        banco = options["banco"]
        cuentas = list(
            CuentaBancaria.objects.filter(banco=banco, activa=True)
            .exclude(numero_cuenta__isnull=True)
            .exclude(numero_cuenta="")
            .order_by("pk")
        )
        cuentas_por_numero = {
            _numero_cuenta_normalizado(cuenta.numero_cuenta): cuenta
            for cuenta in cuentas
            if _numero_cuenta_normalizado(cuenta.numero_cuenta)
        }
        movimientos = list(
            MovimientoBancario.objects.select_related("cuenta")
            .filter(cuenta__in=cuentas)
            .order_by("fecha_transaccion", "pk")
        )
        candidatos = []
        por_destino: dict[str, int] = {}
        for movimiento in movimientos:
            destino = _cuenta_destino_en_descripcion(movimiento, cuentas_por_numero)
            if destino is None:
                continue
            candidatos.append((movimiento, destino))
            numero_destino = str(destino.numero_cuenta or "")
            por_destino[numero_destino] = por_destino.get(numero_destino, 0) + 1

        paired_ids: set[int] = set()
        pairs = 0
        for movimiento, destino in candidatos:
            if movimiento.pk in paired_ids:
                continue
            origen_numero = _numero_cuenta_normalizado(movimiento.cuenta.numero_cuenta)
            contraparte = next(
                (
                    candidate
                    for candidate, candidate_destino in candidatos
                    if candidate.pk not in paired_ids
                    and candidate.pk != movimiento.pk
                    and candidate.cuenta_id == destino.pk
                    and candidate_destino.pk == movimiento.cuenta_id
                    and candidate.tipo != movimiento.tipo
                    and candidate.monto == movimiento.monto
                    and candidate.fecha_transaccion.date() == movimiento.fecha_transaccion.date()
                    and origen_numero in _numero_cuenta_normalizado(candidate.descripcion)
                ),
                None,
            )
            if contraparte is None:
                continue
            paired_ids.update({movimiento.pk, contraparte.pk})
            pairs += 1

        result = {
            "modo": "solo_lectura",
            "banco": banco,
            "cuentas_analizadas": len(cuentas),
            "movimientos_analizados": len(movimientos),
            "movimientos_candidatos": len(candidatos),
            "pares_detectados": pairs,
            "sin_contraparte": len(candidatos) - (pairs * 2),
            "por_cuenta_destino": por_destino,
        }
        self.stdout.write(json.dumps(result, ensure_ascii=False, sort_keys=True))
