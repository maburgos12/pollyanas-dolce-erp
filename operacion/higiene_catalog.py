from __future__ import annotations

from .models import RegistroHigiene


PLANTILLA_VERSION = "2026.1"


def _punto(key: str, seccion: str, etiqueta: str, *, admite_na: bool = False) -> dict:
    return {
        "key": key,
        "seccion": seccion,
        "etiqueta": etiqueta,
        "tipo_respuesta": "CUMPLIMIENTO",
        "admite_na": admite_na,
    }


PLANTILLAS_HIGIENE = {
    RegistroHigiene.TIPO_CLORO_PH: {
        "titulo": "Niveles de cloro y pH",
        "descripcion": "Una toma diaria por punto de muestreo; repite la medición si queda fuera de rango.",
        "version": PLANTILLA_VERSION,
        "puntos": [
            {
                "key": "cloro",
                "seccion": "Agua",
                "etiqueta": "Cloro residual (ppm)",
                "tipo_respuesta": "NUMERICA",
                "opciones": ["0.6", "1", "1.5", "3", "5"],
                "rango_recomendado": ["0.6", "1.5"],
            },
            {
                "key": "ph",
                "seccion": "Agua",
                "etiqueta": "pH",
                "tipo_respuesta": "NUMERICA",
                "opciones": ["6.8", "7.2", "7.6", "7.8", "8.2"],
                "rango_recomendado": ["6.8", "7.6"],
            },
        ],
    },
    RegistroHigiene.TIPO_LIMPIEZA: {
        "titulo": "Programa de limpieza",
        "descripcion": "Revisión diaria agrupada por área; los puntos no conformes piden acción y evidencia.",
        "version": PLANTILLA_VERSION,
        "puntos": [
            _punto("mostrador_repisas", "Mostrador", "Repisas limpias"),
            _punto("mostrador_menu", "Mostrador", "Menú limpio y legible"),
            _punto("mostrador_vitrinas", "Mostrador", "Vitrinas refrigeradas limpias"),
            _punto("mostrador_agua_refrigeradores", "Mostrador", "Sin acumulación de agua en refrigeradores"),
            _punto("mostrador_parrillas", "Mostrador", "Parrillas de refrigeradores sin suciedad"),
            _punto("mostrador_pisos", "Mostrador", "Pisos limpios"),
            _punto("mostrador_iluminacion", "Mostrador", "Lámparas e iluminación limpias"),
            _punto("mostrador_paredes", "Mostrador", "Paredes limpias"),
            _punto("mostrador_apagadores", "Mostrador", "Apagadores limpios"),
            _punto("mostrador_puerta", "Mostrador", "Puerta limpia"),
            _punto("mostrador_ventana", "Mostrador", "Ventanas limpias", admite_na=True),
            _punto("mostrador_entrada", "Mostrador", "Piso de la entrada principal limpio"),
            _punto("mostrador_electronicos", "Mostrador", "Computadora y equipo electrónico limpios"),
            _punto("mostrador_exhibidores", "Mostrador", "Exhibidores y vitrinas secas limpios"),
            _punto("almacen_racks", "Almacén", "Racks limpios y ordenados"),
            _punto("almacen_pisos", "Almacén", "Pisos limpios"),
            _punto("almacen_paredes", "Almacén", "Paredes limpias"),
            _punto("almacen_techos", "Almacén", "Techos limpios"),
            _punto("produccion_mesas", "Producción", "Mesas de trabajo y repisas limpias"),
            _punto("produccion_tarja", "Producción", "Tarja limpia"),
            _punto("produccion_utensilios_lavados", "Producción", "Utensilios lavados"),
            _punto("produccion_utensilios_desinfectados", "Producción", "Utensilios desinfectados antes de usar"),
            _punto("produccion_equipos_limpios", "Producción", "Equipos limpios"),
            _punto("produccion_equipos_desinfectados", "Producción", "Equipos desinfectados antes de usar"),
            _punto("produccion_pisos", "Producción", "Pisos limpios"),
            _punto("produccion_paredes", "Producción", "Paredes limpias"),
            _punto("produccion_techos", "Producción", "Techos limpios"),
            _punto("general_bolleras", "General", "Bolleras limpias"),
            _punto("general_refrigeradores_superior", "General", "Parte superior de refrigeradores limpia"),
            _punto("general_refrigeradores_libres", "General", "Parte superior libre de artículos no aptos", admite_na=True),
            _punto("general_parrillas", "General", "Parrillas de refrigeradores sin suciedad"),
            _punto("general_perecederos", "General", "Insumos perecederos refrigerados y en contenedor cerrado"),
            _punto("general_contenedores_cerrados", "General", "Contenedores refrigerados correctamente cerrados"),
            _punto("general_sin_insumos_piso", "General", "Cajas e insumos separados del piso"),
            _punto("general_sin_insumos_mal_estado", "General", "Sin insumos en mal estado"),
            _punto("general_contenedores_etiquetados", "General", "Contenedores identificados con contenido y fecha"),
            _punto("general_peps", "General", "Insumos ordenados por primeras entradas, primeras salidas"),
            _punto("general_etiquetas", "General", "Etiquetas de insumos completas"),
            _punto("general_insumos_separados", "General", "Limpieza, materia prima y empaque separados y señalizados"),
            _punto("general_sin_olores", "General", "Sin malos olores"),
        ],
    },
    RegistroHigiene.TIPO_BANOS: {
        "titulo": "Limpieza de baños",
        "descripcion": "Cuatro rondas diarias por baño, con limpieza, suministros y seguridad.",
        "version": PLANTILLA_VERSION,
        "puntos": [
            _punto("bano_puerta", "Exterior", "Puerta limpia"),
            _punto("bano_pisos", "Interior", "Pisos limpios"),
            _punto("bano_paredes", "Interior", "Paredes limpias"),
            _punto("bano_techos", "Interior", "Techos limpios"),
            _punto("bano_espejos", "Interior", "Espejos limpios"),
            _punto("bano_lavamanos", "Interior", "Lavamanos limpio y funcional"),
            _punto("bano_interruptores", "Interior", "Interruptores limpios"),
            _punto("bano_sanitario", "Interior", "Sanitario limpio y funcional"),
            _punto("bano_disp_jabon", "Interior", "Dispensador de jabón limpio y con producto"),
            _punto("bano_disp_toallas", "Interior", "Dispensador de toallas limpio y abastecido"),
            _punto("bano_jabon_manos", "Kit de desinfección", "Jabón para manos disponible"),
            _punto("bano_desinfectante_aerosol", "Kit de desinfección", "Desinfectante en aerosol o atomizador disponible"),
            _punto("bano_herramientas", "Kit de desinfección", "Escoba, cepillo, trapeador y cubeta disponibles"),
            _punto("bano_jabon_piso", "Kit de desinfección", "Jabón para piso disponible"),
            _punto("bano_desinfectante", "Kit de desinfección", "Desinfectante para baño disponible"),
            _punto("bano_cubrebocas", "Seguridad", "Personal usa cubrebocas"),
            _punto("bano_guantes", "Seguridad", "Personal usa guantes para la limpieza"),
        ],
    },
}


def plantilla_higiene(tipo: str) -> dict | None:
    return PLANTILLAS_HIGIENE.get(tipo)


def punto_higiene(tipo: str, clave: str) -> tuple[int, dict] | tuple[None, None]:
    plantilla = plantilla_higiene(tipo)
    if not plantilla:
        return None, None
    for orden, punto in enumerate(plantilla["puntos"], start=1):
        if punto["key"] == clave:
            return orden, punto
    return None, None
