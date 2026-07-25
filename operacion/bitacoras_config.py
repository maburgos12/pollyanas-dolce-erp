from __future__ import annotations

from .models import BitacoraOperativa


PASTELES_BASE = [
    "PASTEL CRUNCH MINI",
    "PASTEL 3 PECADOS MINI",
    "PASTEL FRESAS CON CREMA MINI",
    "PASTEL SNICKER'S CH",
    "PASTEL CRUNCH CH",
    "PASTEL ZANAHORIA CH",
    "PASTEL FRESAS CON CREMA CH",
    "PASTEL 3 PECADOS CH",
    "PASTEL SNICKER'S M",
    "PASTEL CRUNCH M",
    "PASTEL ZANAHORIA M",
    "PASTEL FRESAS CON CREMA M",
    "PASTEL 3 LECHES M",
    "PASTEL 3 PECADOS M",
    "PASTEL FRESAS CON CREMA G",
    "PASTEL ZANAHORIA G",
    "PASTEL CRUNCH G",
    "PASTEL SNICKERS G",
    "PASTEL 3 PECADOS G",
]

SIZE_LABELS = {"G": "Grande", "M": "Mediano", "CH": "Chico", "MINI": "Mini", "IND": "Individual", "PIEZA": "Pieza"}


def producto(producto: str, tamano: str, *aliases: str):
    return {"producto": producto, "tamano": tamano, "aliases": list(aliases)}


HORNO_ITEMS = [
    producto("DAWN", "G", "Pan de Vainilla Dawn Grande", "Dawn Grande"),
    producto("DAWN", "M", "Pan de Vainilla Dawn Mediano", "Dawn Mediano"),
    producto("DAWN", "CH", "Pan de Vainilla Dawn Chico", "Dawn Chico"),
    producto("DAWN", "MINI", "Pan de Vainilla Dawn Mini", "Dawn Mini"),
    producto("CHOCOLATE", "G", "Pan Chocolate Grande", "Pan de Chocolate Grande"),
    producto("CHOCOLATE", "M", "Pan Chocolate Mediano", "Pan de Chocolate Mediano"),
    producto("CHOCOLATE", "CH", "Pan Chocolate Chico", "Pan de Chocolate Chico"),
    producto("CHOCOLATE", "MINI", "Pan Chocolate Mini", "Pan de Chocolate Mini"),
    producto("ZANAHORIA", "G", "Pan Zanahoria Grande", "Pan de Zanahoria Grande"),
    producto("ZANAHORIA", "M", "Pan Zanahoria Mediano", "Pan de Zanahoria Mediano"),
    producto("ZANAHORIA", "CH", "Pan Zanahoria Chico", "Pan de Zanahoria Chico"),
    producto("3 LECHES", "M", "Pastel 3 Leches Mediano", "3 Leches Mediano"),
    producto("3 LECHES", "IND", "Pastel 3 Leches Individual", "3 Leches Individual"),
    producto("FLAN", "G", "Flan Grande"),
    producto("FLAN", "M", "Flan Mediano"),
    producto("FLAN", "CH", "Flan Chico"),
    producto("FLAN", "MINI", "Flan Mini"),
    producto("PAY", "G", "Pay Grande"),
    producto("PAY", "M", "Pay Mediano"),
    producto("BOLLO ZANAHORIA", "PIEZA", "Bollo Zanahoria", "Bollo de Zanahoria"),
]

ARMADO_ITEMS = [
    producto("3 LECHES", "M", "Pastel 3 Leches Mediano", "3 Leches Mediano"),
    producto("3 LECHES", "IND", "Pastel 3 Leches Individual", "3 Leches Individual"),
    producto("FRESAS CON CREMA", "G", "Pastel Fresas con Crema Grande"),
    producto("FRESAS CON CREMA", "M", "Pastel Fresas con Crema Mediano"),
    producto("FRESAS CON CREMA", "CH", "Pastel Fresas con Crema Chico"),
    producto("FRESAS CON CREMA", "MINI", "Pastel Fresas con Crema Mini"),
    producto("3 PECADOS", "G", "Pastel 3 Pecados Grande"),
    producto("3 PECADOS", "M", "Pastel 3 Pecados Mediano"),
    producto("3 PECADOS", "CH", "Pastel 3 Pecados Chico"),
    producto("3 PECADOS", "MINI", "Pastel 3 Pecados Mini"),
    producto("ZANAHORIA", "G", "Pastel Zanahoria Grande"),
    producto("ZANAHORIA", "M", "Pastel Zanahoria Mediano"),
    producto("ZANAHORIA", "CH", "Pastel Zanahoria Chico"),
    producto("SNICKERS", "G", "Pastel Snickers Grande"),
    producto("SNICKERS", "M", "Pastel Snickers Mediano"),
    producto("SNICKERS", "CH", "Pastel Snickers Chico"),
    producto("CRUNCH", "G", "Pastel Crunch Grande"),
    producto("CRUNCH", "M", "Pastel Crunch Mediano"),
    producto("CRUNCH", "CH", "Pastel Crunch Chico"),
    producto("CRUNCH", "MINI", "Pastel Crunch Mini"),
]

TEMPERATURA_EQUIPOS = [
    ("FP 1", "-18", "-5"),
    ("RP 1", "4", "9"),
    ("RP 1.1", "4", "9"),
    ("CFP 1", "4", "9"),
    ("CFP 1.1", "4", "9"),
    ("CFA 1", "4", "9"),
]

INOCUIDAD_ITEMS = [
    ("PERSONAL", "Todo el personal lleva uniforme limpio y completo."),
    ("PERSONAL", "Manos lavadas y desinfectadas según procedimiento."),
    ("PERSONAL", "Sin joyería, maquillaje excesivo o uñas largas/esmalte."),
    ("INSTALACIONES", "Área de producción limpia y desinfectada antes de iniciar."),
    ("INSTALACIONES", "Superficies y equipos libres de residuos previos."),
    ("INSTALACIONES", "Lavamanos abastecidos con jabón, papel y desinfectante."),
    ("MATERIA PRIMA", "Materias primas recibidas sin daños y con caducidad correcta."),
    ("MATERIA PRIMA", "Almacenamiento correcto por temperatura y tipo."),
    ("PROCESO", "Control de temperaturas durante la preparación."),
    ("PROCESO", "Preparación realizada según receta estándar."),
    ("PLAGAS", "Revisión visual diaria de signos de plagas."),
    ("DOCUMENTACIÓN", "Registros de limpieza, temperaturas y bitácoras actualizados."),
]

LIMPIEZA_ITEMS = [
    ("Durante la jornada", "Limpiar área de trabajo al iniciar y terminar actividad."),
    ("Durante la jornada", "Mantener utensilios e insumos organizados y limpios."),
    ("Durante la jornada", "Retirar restos de ingredientes o líquidos derramados."),
    ("Durante la jornada", "No dejar insumos abiertos."),
    ("Refrigerador", "Limpiar diariamente el espacio asignado del refrigerador."),
    ("Refrigerador", "No poner insumos abiertos o sin etiqueta dentro del refrigerador."),
    ("Cierre", "Retirar y desechar residuos sólidos y restos de alimentos."),
    ("Cierre", "Dejar utensilios limpios y en su lugar."),
]

BITACORA_CONFIG = {
    BitacoraOperativa.TIPO_INVENTARIO_CFP1: {
        "titulo": "Inventario diario CFP1",
        "ayuda": "Captura CEDIS, devolución, total y comentarios por producto.",
        "familia": "daily_product",
        "campos": ["cedis", "devolucion", "total"],
        "items": PASTELES_BASE,
    },
    BitacoraOperativa.TIPO_ROTACION: {
        "titulo": "Rotación producto",
        "ayuda": "Registra producto, cantidad, fecha del producto y día.",
        "familia": "daily_product",
        "campos": ["cantidad", "fecha_producto", "dia"],
        "items": PASTELES_BASE,
    },
    BitacoraOperativa.TIPO_REBANADO: {
        "titulo": "Producto rebanado",
        "ayuda": "Captura enteros, rebanadas, merma y motivo.",
        "familia": "daily_product",
        "campos": ["pastel_entero", "total_rebanadas", "merma_rebanadas", "fecha_producto", "motivo_merma"],
        "items": PASTELES_BASE,
    },
    BitacoraOperativa.TIPO_MERMA: {
        "titulo": "Registro de merma",
        "ayuda": "Merma general de insumos o materia prima.",
        "familia": "free_rows",
        "campos": ["item", "unidad", "cantidad", "motivo", "responsable", "hora"],
    },
    BitacoraOperativa.TIPO_RECHAZOS: {
        "titulo": "Rechazos por sucursal",
        "ayuda": "Rechazos de calidad reportados por sucursal.",
        "familia": "free_rows",
        "campos": ["item", "unidad", "cantidad", "sucursal_texto", "motivo", "responsable", "hora"],
    },
    BitacoraOperativa.TIPO_TEMPERATURA: {
        "titulo": "Bitácora de temperatura",
        "ayuda": "Temperatura de apertura y cierre por equipo, con rango visible.",
        "familia": "temperature",
        "campos": ["apertura", "cierre"],
        "equipos": TEMPERATURA_EQUIPOS,
    },
    BitacoraOperativa.TIPO_HORNOS: {
        "titulo": "Control producción - Hornos",
        "ayuda": "Captura por día: stock, existencia, pedido, proyección y preparación.",
        "familia": "weekly_matrix",
        "campos": ["stock", "existencia", "pedido", "proyeccion", "preparacion"],
        "items": HORNO_ITEMS,
    },
    BitacoraOperativa.TIPO_ARMADO: {
        "titulo": "Control producción - Armado",
        "ayuda": "Embetunado y armado por día, producto y tamaño.",
        "familia": "weekly_matrix",
        "campos": ["stock", "existencia", "pedido", "proyeccion", "preparacion"],
        "items": ARMADO_ITEMS,
    },
    BitacoraOperativa.TIPO_INOCUIDAD: {
        "titulo": "Checklist inocuidad producción",
        "ayuda": "Revisión de inocuidad por sección y día.",
        "familia": "checklist",
        "items": INOCUIDAD_ITEMS,
    },
    BitacoraOperativa.TIPO_LIMPIEZA: {
        "titulo": "Checklist limpieza",
        "ayuda": "Actividades de limpieza con cumple/no cumple y comentarios.",
        "familia": "checklist",
        "items": LIMPIEZA_ITEMS,
    },
}


BITACORA_GROUPS = [
    (
        "Producción diaria",
        [
            BitacoraOperativa.TIPO_INVENTARIO_CFP1,
            BitacoraOperativa.TIPO_ROTACION,
            BitacoraOperativa.TIPO_REBANADO,
            BitacoraOperativa.TIPO_MERMA,
            BitacoraOperativa.TIPO_RECHAZOS,
        ],
    ),
    ("Producción semanal", [BitacoraOperativa.TIPO_HORNOS, BitacoraOperativa.TIPO_ARMADO]),
    ("Calidad e inocuidad", [BitacoraOperativa.TIPO_TEMPERATURA, BitacoraOperativa.TIPO_INOCUIDAD, BitacoraOperativa.TIPO_LIMPIEZA]),
]
