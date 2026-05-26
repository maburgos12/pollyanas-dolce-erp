from __future__ import annotations

from dataclasses import dataclass

from recetas.utils.normalizacion import normalizar_nombre

from .models import Empleado


PUESTO_HORNOS = "HORNOS"
PUESTO_PRODUCCION = "PRODUCCION"
PUESTO_ARMADO = "ARMADO"
PUESTO_CRUCERO = "CRUCERO"
PUESTO_ENVIO_SUCURSAL = "ENVIO_SUCURSAL"
PUESTO_REPARTIDOR = "REPARTIDOR"
PUESTO_LIMPIEZA = "LIMPIEZA"
PUESTO_MANTENIMIENTO = "MANTENIMIENTO"
PUESTO_ALMACEN = "ALMACEN"
PUESTO_CALL_CENTER = "CALL_CENTER"
PUESTO_JEFATURA = "JEFATURA"
PUESTO_RRHH = "RRHH"


@dataclass(frozen=True)
class EmpleadoRegla:
    nombre: str
    departamento: str
    puesto: str
    puesto_operativo: str = ""
    jefe_nombre: str = ""
    participa_bonos_ventas: bool = False
    participa_bonos_produccion: bool = False
    tipo_personal: str = Empleado.TIPO_POLLYANA
    departamento_origen: str = ""


REGLAS_NOMINALES = [
    EmpleadoRegla(
        "SOTO INZUNZA YESENIA",
        Empleado.DEP_ADMINISTRACION,
        "Jefe de Administración",
        PUESTO_JEFATURA,
    ),
    EmpleadoRegla(
        "LOPEZ PALOS JOHANA ADELIN",
        Empleado.DEP_VENTAS,
        "Jefe de Ventas",
        PUESTO_JEFATURA,
    ),
    EmpleadoRegla(
        "CAYETANO VALENZUELA CAROLINA",
        Empleado.DEP_PRODUCCION,
        "Jefe de Producción",
        PUESTO_JEFATURA,
    ),
    EmpleadoRegla(
        "LUGO ESPINOZA PAULA ELIZABETH",
        Empleado.DEP_RRHH,
        "Jefe de Recursos Humanos",
        PUESTO_RRHH,
    ),
    EmpleadoRegla(
        "RIVAS SOLIS ROXANA",
        Empleado.DEP_PRODUCCION,
        "Supervisora de Producción",
        "SUPERVISION_PRODUCCION",
        "CAYETANO VALENZUELA CAROLINA",
    ),
    EmpleadoRegla(
        "ANGULO PARRA JULISSA",
        Empleado.DEP_PRODUCCION,
        "Encargada de Producción",
        "ENCARGADA_PRODUCCION",
        "CAYETANO VALENZUELA CAROLINA",
    ),
    EmpleadoRegla(
        "PEREZ VALENZUELA JORGE ISAAC",
        Empleado.DEP_MANTENIMIENTO,
        "Mantenimiento",
        PUESTO_MANTENIMIENTO,
        "SOTO INZUNZA YESENIA",
    ),
]


def _nombre_key(nombre: str) -> str:
    return normalizar_nombre(nombre or "")


def _index_por_nombre() -> dict[str, Empleado]:
    return {empleado.nombre_normalizado: empleado for empleado in Empleado.objects.all()}


def _inferir_puesto_operativo(empleado: Empleado) -> str:
    area = (empleado.area or "").upper()
    puesto = (empleado.puesto or "").upper()
    texto = f"{area} {puesto}"
    if "HORNO" in texto:
        return PUESTO_HORNOS
    if "ARMADO" in texto:
        return PUESTO_ARMADO
    if "CRUCERO" in puesto and "VENTAS" not in area:
        return PUESTO_CRUCERO
    if "ENVIO" in texto or "ENVÍO" in texto or "SUCURSAL" in texto:
        return PUESTO_ENVIO_SUCURSAL
    if "REPART" in texto:
        return PUESTO_REPARTIDOR
    if "CALL" in texto:
        return PUESTO_CALL_CENTER
    if "AFAN" in texto or "LIMPIEZA" in texto:
        return PUESTO_LIMPIEZA
    if "ALMACEN" in texto or "ALMACÉN" in texto:
        return PUESTO_ALMACEN
    if "PRODUCCION" in area or "PRODUCCIÓN" in area:
        return PUESTO_PRODUCCION
    return ""


def _base_por_area(empleado: Empleado, jefes: dict[str, Empleado]) -> dict:
    area = (empleado.area or "").upper().strip()
    puesto = (empleado.puesto or "").upper().strip()
    puesto_operativo = _inferir_puesto_operativo(empleado)
    data = {
        "departamento_origen": "",
        "tipo_personal": Empleado.TIPO_POLLYANA,
        "puesto_operativo": puesto_operativo,
        "participa_bonos_ventas": False,
        "participa_bonos_produccion": False,
    }
    if "MARKETING" in area:
        data["departamento_origen"] = Empleado.DEP_MARKETING
        data["departamento"] = Empleado.DEP_MARKETING
        data["tipo_personal"] = Empleado.TIPO_EXTERNO
    elif "REPART" in area or puesto_operativo == PUESTO_REPARTIDOR:
        data["departamento_origen"] = Empleado.DEP_LOGISTICA
        data["departamento"] = Empleado.DEP_VENTAS
        data["puesto"] = "Repartidor"
        data["jefe_directo"] = jefes.get("johana")
        data["participa_bonos_ventas"] = True
    elif "VENTAS" in area:
        data["departamento_origen"] = Empleado.DEP_VENTAS
        data["departamento"] = Empleado.DEP_VENTAS
        data["jefe_directo"] = jefes.get("johana")
        data["participa_bonos_ventas"] = True
        if "CALL" in puesto:
            data["puesto_operativo"] = PUESTO_CALL_CENTER
    elif "HORNOS" in area or "PRODUCCION" in area or "PRODUCCIÓN" in area:
        data["departamento_origen"] = Empleado.DEP_PRODUCCION
        data["departamento"] = Empleado.DEP_PRODUCCION
        data["jefe_directo"] = jefes.get("carolina")
        data["participa_bonos_produccion"] = True
    elif "LOGISTICA" in area and puesto_operativo == PUESTO_ENVIO_SUCURSAL:
        data["departamento_origen"] = Empleado.DEP_LOGISTICA
        data["departamento"] = Empleado.DEP_PRODUCCION
        data["jefe_directo"] = jefes.get("carolina")
        data["participa_bonos_produccion"] = True
    elif "AFAN" in area or "LIMPIEZA" in area:
        data["departamento_origen"] = Empleado.DEP_ADMINISTRACION
        data["departamento"] = Empleado.DEP_ADMINISTRACION
        data["puesto"] = "Limpieza"
        data["jefe_directo"] = jefes.get("yesenia")
    elif "ALMACEN" in area or "ALMACÉN" in area:
        data["departamento_origen"] = Empleado.DEP_ADMINISTRACION
        data["departamento"] = Empleado.DEP_ADMINISTRACION
        data["puesto_operativo"] = puesto_operativo or PUESTO_ALMACEN
        data["jefe_directo"] = jefes.get("yesenia")
    elif "ADMIN" in area:
        data["departamento_origen"] = Empleado.DEP_ADMINISTRACION
        data["departamento"] = Empleado.DEP_ADMINISTRACION
        data["jefe_directo"] = jefes.get("yesenia")
    return data


def _aplicar(empleado: Empleado, data: dict) -> bool:
    changed = False
    for field, value in data.items():
        if value is None:
            continue
        if getattr(empleado, field) != value:
            setattr(empleado, field, value)
            changed = True
    if changed:
        empleado.save(
            update_fields=[
                "departamento",
                "departamento_origen",
                "puesto",
                "puesto_operativo",
                "jefe_directo",
                "tipo_personal",
                "participa_bonos_ventas",
                "participa_bonos_produccion",
                "updated_at",
            ]
        )
    return changed


def aplicar_estructura_organizacional_inicial() -> dict[str, int]:
    empleados = _index_por_nombre()
    jefes = {
        "yesenia": empleados.get(_nombre_key("SOTO INZUNZA YESENIA")),
        "johana": empleados.get(_nombre_key("LOPEZ PALOS JOHANA ADELIN")),
        "carolina": empleados.get(_nombre_key("CAYETANO VALENZUELA CAROLINA")),
        "roxana": empleados.get(_nombre_key("RIVAS SOLIS ROXANA")),
    }
    actualizados = 0

    for regla in REGLAS_NOMINALES:
        empleado = empleados.get(_nombre_key(regla.nombre))
        if not empleado:
            continue
        jefe = empleados.get(_nombre_key(regla.jefe_nombre)) if regla.jefe_nombre else None
        if _aplicar(
            empleado,
            {
                "departamento": regla.departamento,
                "departamento_origen": regla.departamento_origen or regla.departamento,
                "puesto": regla.puesto,
                "puesto_operativo": regla.puesto_operativo,
                "jefe_directo": jefe,
                "tipo_personal": regla.tipo_personal,
                "participa_bonos_ventas": regla.participa_bonos_ventas,
                "participa_bonos_produccion": regla.participa_bonos_produccion,
            },
        ):
            actualizados += 1

    for empleado in Empleado.objects.all():
        if empleado.nombre_normalizado in {_nombre_key(regla.nombre) for regla in REGLAS_NOMINALES}:
            continue
        data = _base_por_area(empleado, jefes)
        if data and _aplicar(empleado, data):
            actualizados += 1

    return {"actualizados": actualizados, "total": Empleado.objects.count()}
