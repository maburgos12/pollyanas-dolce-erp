from __future__ import annotations

# Point es la fuente única de la familia/categoría real (sincronizada
# directo del campo "Familia" de Point en product_recipe_sync_service.py) —
# no se normaliza automáticamente por mayúsculas/minúsculas ni texto
# parecido (ej. "GALLETAS" y "Galletas" se dejan tal cual, aunque sean el
# mismo texto con distinta capitalización — esa inconsistencia ya viene de
# Point mismo, no se inventa una fusión genérica sobre datos ajenos).
#
# Qué familias son "el mismo grupo" en la práctica (mismas áreas de
# Hornos/Armado/Embetunado) es una decisión de negocio, no una regla de
# texto automática — vive en reportes.models.FamiliaGrupoManoObra, editable
# desde la pantalla de clasificación por quien conozca el proceso
# (Carolina), no en este módulo.

from reportes.models import FamiliaGrupoManoObra


def grupo_de_familia(familia: str) -> str:
    """Nombre del grupo canónico que se clasifica y se muestra en pantalla
    para una familia/categoría real de Point. Si la familia no está
    fusionada a ningún grupo, el grupo es ella misma (sin fusionar nada)."""
    fila = FamiliaGrupoManoObra.objects.filter(familia_real=familia).first()
    return fila.grupo if fila else familia


def familias_del_grupo(grupo: str) -> list[str]:
    """Familias/categorías reales de Point que representa un grupo
    canónico — para consultar Receta.familia/Insumo.categoria."""
    familias = list(
        FamiliaGrupoManoObra.objects.filter(grupo=grupo).values_list("familia_real", flat=True)
    )
    return familias or [grupo]
