"""Public projection of Point's active final-product catalog, never its insumos."""
from __future__ import annotations

import re

from pos_bridge.utils.exceptions import ExtractionError


def customer_products(rows: list[dict]) -> list[dict]:
    products = []
    codes, identifiers = set(), set()
    for row in rows:
        for flag in ('Activo', 'IsInsumo', 'Servicio', 'Complemento'):
            if flag in row and type(row[flag]) is not bool:
                raise ExtractionError('Point devolvió clasificación de producto inválida.')
        if row.get('Activo') is False or any(row.get(flag) is True for flag in ('IsInsumo', 'Servicio', 'Complemento')):
            continue
        identifier = row.get('PK_Producto', row.get('PK'))
        code, name = row.get('Codigo'), row.get('Nombre')
        if (
            type(identifier) not in (str, int) or len(str(identifier)) > 30 or not re.fullmatch(r'[1-9][0-9]*', str(identifier))
            or not isinstance(code, str) or not code.strip() or len(code) > 120
            or not isinstance(name, str) or not name.strip() or len(name) > 200
            or any(str(row[key]) != str(identifier) for key in ('PK_Producto', 'PK') if key in row)
            or (row.get('Categoria') is not None and (not isinstance(row['Categoria'], str) or len(row['Categoria']) > 120))
            or code.casefold() in codes or str(identifier) in identifiers
        ):
            raise ExtractionError('Point devolvió identidades de catálogo ambiguas o inválidas.')
        codes.add(code.casefold())
        identifiers.add(str(identifier))
        products.append({
            'point_product_id': str(identifier), 'product_code': code, 'product_name': name,
            'category': row.get('Categoria') if isinstance(row.get('Categoria'), str) else None,
            'presentation': None,
        })
    return products
