"""Vinculación de reportes repetidos al reporte que ya se está atendiendo.

Un mismo problema (la llanta de una unidad, el horno que no calienta) se reporta
varias veces antes de que alguien lo atienda. Borrar los repetidos elimina la
evidencia de que el problema es recurrente, que es justo el dato que sirve para
decidir un cambio de fondo. En vez de eso se ligan al principal: salen de la
bandeja de pendientes, siguen contando y se cierran junto con él.
"""

import logging

from django.db import transaction

from .models import BitacoraFalla, ReporteFalla

logger = logging.getLogger(__name__)

MAX_SALTOS = 20


class DuplicadoInvalido(Exception):
    """La vinculación pedida dejaría la cadena de duplicados inconsistente."""


def principal_de(reporte: ReporteFalla) -> ReporteFalla:
    """Sube por la cadena hasta el reporte que realmente se atiende.

    Mantiene la jerarquía plana: marcar algo como duplicado de un duplicado liga
    al principal de ambos. El tope de saltos es defensa contra un ciclo que
    hubiera quedado en la base por una edición manual.
    """

    actual = reporte
    for _ in range(MAX_SALTOS):
        if actual.duplicado_de_id is None:
            return actual
        actual = actual.duplicado_de
    raise DuplicadoInvalido("La cadena de duplicados es demasiado larga; revisa los reportes ligados.")


@transaction.atomic
def marcar_duplicado(reporte: ReporteFalla, principal: ReporteFalla, usuario) -> ReporteFalla:
    """Liga `reporte` al `principal` y devuelve el principal efectivo."""

    reporte = ReporteFalla.objects.select_for_update().get(pk=reporte.pk)
    principal = ReporteFalla.objects.select_for_update().get(pk=principal.pk)

    if reporte.pk == principal.pk:
        raise DuplicadoInvalido("Un reporte no puede ser duplicado de sí mismo.")

    destino = principal_de(principal)
    if destino.pk == reporte.pk:
        raise DuplicadoInvalido(
            f"La falla #{principal.pk} ya está ligada a #{reporte.pk}; ligarlas al revés crearía un ciclo."
        )
    if reporte.estatus in (ReporteFalla.ESTATUS_CERRADO, ReporteFalla.ESTATUS_CANCELADO):
        raise DuplicadoInvalido("El reporte ya está cerrado; no hace falta ligarlo.")

    # Si el reporte ya tenía duplicados colgando, se mueven al mismo principal
    # para que no quede una cadena de dos niveles.
    ReporteFalla.objects.filter(duplicado_de=reporte).update(duplicado_de=destino)

    reporte.duplicado_de = destino
    reporte.save(update_fields=["duplicado_de"])

    BitacoraFalla.objects.create(
        reporte=reporte,
        usuario=usuario,
        comentario=f"Mismo problema que la falla #{destino.pk} ({destino.titulo}); el seguimiento continúa ahí.",
    )
    BitacoraFalla.objects.create(
        reporte=destino,
        usuario=usuario,
        comentario=f"Se ligó la falla #{reporte.pk} como reporte repetido de este mismo problema.",
    )

    # El try va DENTRO del callback: on_commit corre después de cerrar la
    # transacción, así que un broker caído aquí reventaría la petición cuando la
    # vinculación ya quedó guardada.
    def _avisar_al_reportante():
        try:
            from .tasks import notificar_duplicado_vinculado

            notificar_duplicado_vinculado.delay(reporte.pk, destino.pk)
        except Exception as exc:  # el aviso es accesorio; la vinculación ya está hecha
            logger.warning("[fallas] No se pudo encolar el aviso de duplicado %s: %s", reporte.pk, exc)

    transaction.on_commit(_avisar_al_reportante)

    return destino


def propagar_cierre(principal: ReporteFalla, usuario, estatus: str) -> list[int]:
    """Arrastra a los duplicados cuando el principal se resuelve, cierra o cancela.

    Devuelve los ids movidos para que la vista notifique a quienes los reportaron.
    """

    duplicados = list(ReporteFalla.objects.filter(duplicado_de=principal).exclude(estatus=estatus))
    for duplicado in duplicados:
        duplicado.estatus = estatus
        campos = ["estatus"]
        if estatus in (ReporteFalla.ESTATUS_CERRADO, ReporteFalla.ESTATUS_CANCELADO):
            duplicado.fecha_cierre = principal.fecha_cierre
            duplicado.cerrado_por = usuario
            campos += ["fecha_cierre", "cerrado_por"]
        if estatus == ReporteFalla.ESTATUS_RESUELTO and not duplicado.fecha_resolucion:
            duplicado.fecha_resolucion = principal.fecha_resolucion
            campos.append("fecha_resolucion")
        duplicado.save(update_fields=campos)
        BitacoraFalla.objects.create(
            reporte=duplicado,
            usuario=usuario,
            estatus_nuevo=estatus,
            comentario=f"Se actualizó junto con la falla #{principal.pk}, que atiende este mismo problema.",
        )
    return [duplicado.pk for duplicado in duplicados]
