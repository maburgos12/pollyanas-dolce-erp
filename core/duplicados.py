"""Árbol de reportes repetidos, compartido por fallas y logística.

Ambas apps ligan reportes repetidos a uno principal con un campo `duplicado_de`
autorreferenciado. La parte delicada —resolver la raíz, impedir ciclos y dejar
la jerarquía plana— vive aquí una sola vez; cada app se encarga de su propia
bitácora y de sus avisos, que son distintos.
"""

MAX_SALTOS = 20


class DuplicadoInvalido(Exception):
    """La vinculación pedida dejaría la cadena de duplicados inconsistente."""


def principal_de(reporte):
    """Sube por la cadena hasta el reporte que realmente se atiende.

    El tope de saltos es defensa contra un ciclo que hubiera quedado en la base
    por una edición manual.
    """

    actual = reporte
    for _ in range(MAX_SALTOS):
        if actual.duplicado_de_id is None:
            return actual
        actual = actual.duplicado_de
    raise DuplicadoInvalido("La cadena de duplicados es demasiado larga; revisa los reportes ligados.")


def enlazar_duplicado(reporte, principal, *, estatus_cerrados=()):
    """Liga `reporte` al principal efectivo de `principal` y lo devuelve.

    No guarda bitácora ni notifica: eso lo decide cada app.
    """

    modelo = type(reporte)
    if reporte.pk == principal.pk:
        raise DuplicadoInvalido("Un reporte no puede ser duplicado de sí mismo.")

    destino = principal_de(principal)
    if destino.pk == reporte.pk:
        raise DuplicadoInvalido(
            f"El reporte #{principal.pk} ya está ligado a #{reporte.pk}; ligarlos al revés crearía un ciclo."
        )
    if reporte.estatus in estatus_cerrados:
        raise DuplicadoInvalido("El reporte ya está cerrado; no hace falta ligarlo.")

    # Si el reporte ya tenía repetidos colgando, se mueven al mismo principal
    # para que no quede una cadena de dos niveles.
    modelo.objects.filter(duplicado_de=reporte).update(duplicado_de=destino)

    reporte.duplicado_de = destino
    reporte.save(update_fields=["duplicado_de"])
    return destino
