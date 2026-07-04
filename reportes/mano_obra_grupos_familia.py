from __future__ import annotations

# Point es la fuente única de la familia/categoría real (sincronizada
# directo del campo "Familia" de Point en product_recipe_sync_service.py) —
# no se normaliza automáticamente por mayúsculas/minúsculas ni texto
# parecido (ej. "GALLETAS" y "Galletas" se dejan tal cual, aunque sean el
# mismo texto con distinta capitalización — esa inconsistencia ya viene de
# Point mismo, no se inventa una fusión genérica sobre datos ajenos).
#
# Estos son los ÚNICOS 2 casos donde Mauricio confirmó que dos familias
# distintas de Point son, en la práctica, el mismo grupo de producción
# (mismas áreas de Hornos/Armado/Embetunado) — decisión de negocio
# explícita, no una regla de texto automática. Agregar aquí solo casos
# confirmados así, uno por uno.
GRUPOS_FAMILIA_MANO_OBRA: dict[str, list[str]] = {
    "Pastel": ["Pastel", "Pastel Chico", "Pastel Grande", "Pastel Mediano", "Pastel Mini"],
    "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)": [
        "Betún y Rellenos",
        "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)",
    ],
}


def grupo_de_familia(familia: str) -> str:
    """Nombre del grupo canónico que se clasifica y se muestra en pantalla
    para una familia/categoría real de Point. Si la familia no está en
    ningún grupo conocido, el grupo es ella misma (sin fusionar nada)."""
    for grupo, miembros in GRUPOS_FAMILIA_MANO_OBRA.items():
        if familia in miembros:
            return grupo
    return familia


def familias_del_grupo(grupo: str) -> list[str]:
    """Familias/categorías reales de Point que representa un grupo
    canónico — para consultar Receta.familia/Insumo.categoria."""
    return GRUPOS_FAMILIA_MANO_OBRA.get(grupo, [grupo])
