BRANCH_SELECT_CANDIDATES = [
    "#slctAlmacenPA",
    "select#slctAlmacenPA",
    "select[name*='sucursal']",
    "select[name*='branch']",
    "select[name*='store']",
    "label:has-text('Sucursal') + select",
]

INVENTORY_TABLE_CANDIDATES = [
    "#tablaProductosPA",
    "table#tablaProductosPA",
    "table[data-testid='inventory-table']",
    "table",
    "[role='table']",
]

INSUMOS_TABLE_CANDIDATES = [
    "#tablaInsumosPA",
    "table#tablaInsumosPA",
]

NEXT_PAGE_CANDIDATES = [
    "#tablaProductosPA_next",
    "#tablaProductosPA_next a",
    "button[aria-label='Next']",
    "button:has-text('Siguiente')",
    "a[rel='next']",
]

PRODUCT_CATEGORY_SELECT_CANDIDATES = [
    "#slctCategoriaPA",
    "select#slctCategoriaPA",
]

SUPPLY_CATEGORY_SELECT_CANDIDATES = [
    "#slctCatInsumoPA",
    "select#slctCatInsumoPA",
]

HEADER_ALIASES = {
    "external_id": ["id", "codigo", "cve", "clave", "clave producto", "id producto"],
    "sku": ["sku", "codigo sku", "codigo producto", "codigo"],
    "name": ["producto", "nombre", "descripcion", "articulo"],
    "category": ["categoria", "familia", "grupo", "linea"],
    "stock": ["existencia", "stock", "inventario", "cantidad", "actual"],
    "min_stock": ["minimo", "stock minimo", "reorden minimo", "min"],
    "max_stock": ["maximo", "stock maximo", "reorden maximo", "max"],
}
