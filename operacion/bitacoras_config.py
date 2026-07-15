from recetas.models import Receta

from .models import BitacoraOperativa


BITACORA_CONFIG = {
    BitacoraOperativa.TIPO_SALIDAS_CFP1: {
        "titulo": "Salidas CFP1",
        "ayuda": "Cantidades enviadas por producto a cada sucursal.",
        "campos": ["cantidad"],
        "usa_sucursales": True,
    },
    BitacoraOperativa.TIPO_INVENTARIO_CFP1: {
        "titulo": "Inventario CFP1",
        "ayuda": "Existencia CEDIS y devolución del día.",
        "campos": ["cedis", "devolucion"],
    },
    BitacoraOperativa.TIPO_PLAGAS: {
        "titulo": "Control de plagas",
        "ayuda": "Registro de detección o aplicación.",
        "campos": ["plaga", "area", "metodo", "fecha_deteccion"],
        "sin_producto": True,
    },
    BitacoraOperativa.TIPO_HORNOS: {
        "titulo": "Control producción - Hornos",
        "ayuda": "Existencia y preparación por receta.",
        "familia": "produccion_lotes",
        "campos": ["existencia", "preparacion"],
        "receta_tipo": Receta.TIPO_PREPARACION,
        "requiere_codigo_point": True,
    },
    BitacoraOperativa.TIPO_CFP11: {
        "titulo": "Inventario CFP 1.1",
        "ayuda": "Existencia física y salida a Armado por producto Point.",
        "familia": "custodia_lotes",
        "campos": ["existencia_fisica", "salida_armado"],
        "campos_legacy": ["bloque", "tamano", "existencia", "salida", "entrada"],
        "usa_insumos": True,
        "requiere_codigo_point": True,
    },
    BitacoraOperativa.TIPO_ARMADO: {
        "titulo": "Control producción - Armado",
        "ayuda": "Consumo real y producto terminado por receta.",
        "familia": "transformacion_lotes",
        "campos": ["consumo_real", "producto_terminado"],
        "receta_tipo": Receta.TIPO_PRODUCTO_FINAL,
        "requiere_codigo_point": True,
    },
    BitacoraOperativa.TIPO_ROTACION: {
        "titulo": "Rotación producto",
        "ayuda": "Producto, cantidad y fecha del producto.",
        "campos": ["cantidad", "fecha_producto"],
    },
    BitacoraOperativa.TIPO_REBANADO: {
        "titulo": "Producto rebanado",
        "ayuda": "Enteros, rebanadas y merma.",
        "campos": ["pastel_entero", "total_rebanadas", "merma_rebanadas", "fecha_producto", "motivo_merma"],
    },
}
