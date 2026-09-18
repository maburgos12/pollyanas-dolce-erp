"""Resolve counting units on the server, never from an operator's text."""
from pos_bridge.models import PointProduct
from pos_bridge.services.product_count_units import product_count_unit


def unidad_conteo(source):
    if isinstance(source, PointProduct):
        return product_count_unit(source)
    if source.unidad_base_id:
        return source.unidad_base.codigo, 'Unidad base del catálogo canónico'
    return '', ''
