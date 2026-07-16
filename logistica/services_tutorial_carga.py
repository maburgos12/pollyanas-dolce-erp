from datetime import datetime
from zoneinfo import ZoneInfo

from logistica.models import Repartidor, RutaEntrega


TUTORIAL_CARGA_SUCURSAL_LANZAMIENTO = datetime(
    2026,
    7,
    16,
    0,
    0,
    tzinfo=ZoneInfo("America/Mazatlan"),
)


def debe_mostrar_tutorial_carga(repartidor: Repartidor | None) -> bool:
    """Muestra la novedad una vez a cuentas existentes sin interrumpir rutas activas."""
    if not repartidor or repartidor.tutorial_carga_sucursal_visto_en:
        return False
    if repartidor.user.date_joined > TUTORIAL_CARGA_SUCURSAL_LANZAMIENTO:
        return False
    return not RutaEntrega.objects.filter(
        repartidor=repartidor,
        estatus=RutaEntrega.ESTATUS_EN_RUTA,
    ).exists()
