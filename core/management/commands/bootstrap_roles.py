from django.core.management.base import BaseCommand
from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.models import ContentType

ROLE_PERMS = {
    "DG": [
        # Sprint 1: solo lectura
        "core.view_auditlog",
        "recetas.view_receta",
        "recetas.view_lineareceta",
        "recetas.view_planproduccion",
        "recetas.view_planproduccionitem",
        "maestros.view_insumo",
        "maestros.view_costoinsumo",
        "crm.view_cliente",
        "crm.view_pedidocliente",
        "crm.view_seguimientopedido",
        "rrhh.view_empleado",
        "rrhh.view_nominaperiodo",
        "rrhh.view_nominalinea",
    ],
    "ADMIN": [
        "core.view_auditlog",
        "recetas.view_receta",
        "recetas.view_lineareceta",
        "recetas.change_lineareceta",  # aprobar matching
        "recetas.view_planproduccion",
        "recetas.add_planproduccion",
        "recetas.change_planproduccion",
        "recetas.delete_planproduccion",
        "recetas.view_planproduccionitem",
        "recetas.add_planproduccionitem",
        "recetas.change_planproduccionitem",
        "recetas.delete_planproduccionitem",
        "maestros.view_insumo",
        "maestros.view_costoinsumo",
        "compras.view_solicitudcompra",
        "compras.add_solicitudcompra",
        "compras.change_solicitudcompra",
        "compras.view_ordencompra",
        "compras.add_ordencompra",
        "compras.change_ordencompra",
        "compras.view_recepcioncompra",
        "compras.add_recepcioncompra",
        "compras.change_recepcioncompra",
        "inventario.view_existenciainsumo",
        "inventario.add_existenciainsumo",
        "inventario.change_existenciainsumo",
        "inventario.view_movimientoinventario",
        "inventario.add_movimientoinventario",
        "inventario.change_movimientoinventario",
        "inventario.view_ajusteinventario",
        "inventario.add_ajusteinventario",
        "inventario.change_ajusteinventario",
        "crm.view_cliente",
        "crm.add_cliente",
        "crm.change_cliente",
        "crm.view_pedidocliente",
        "crm.add_pedidocliente",
        "crm.change_pedidocliente",
        "crm.view_seguimientopedido",
        "crm.add_seguimientopedido",
        "crm.change_seguimientopedido",
        "rrhh.view_empleado",
        "rrhh.add_empleado",
        "rrhh.change_empleado",
        "rrhh.view_nominaperiodo",
        "rrhh.add_nominaperiodo",
        "rrhh.change_nominaperiodo",
        "rrhh.view_nominalinea",
        "rrhh.add_nominalinea",
        "rrhh.change_nominalinea",
    ],
    "COMPRAS": [
        "recetas.view_receta",
        "recetas.view_lineareceta",
        "recetas.view_planproduccion",
        "recetas.view_planproduccionitem",
        "maestros.add_insumo",
        "maestros.change_insumo",
        "maestros.add_costoinsumo",
        "maestros.change_costoinsumo",
        "maestros.view_insumo",
        "maestros.view_costoinsumo",
        "compras.view_solicitudcompra",
        "compras.add_solicitudcompra",
        "compras.change_solicitudcompra",
        "compras.view_ordencompra",
        "compras.add_ordencompra",
        "compras.change_ordencompra",
        "compras.view_recepcioncompra",
        "compras.add_recepcioncompra",
        "compras.change_recepcioncompra",
    ],
    "ALMACEN": [
        "recetas.view_receta",
        "recetas.view_lineareceta",
        "maestros.view_insumo",
        "maestros.view_costoinsumo",
        "inventario.view_existenciainsumo",
        "inventario.add_existenciainsumo",
        "inventario.change_existenciainsumo",
        "inventario.view_movimientoinventario",
        "inventario.add_movimientoinventario",
        "inventario.change_movimientoinventario",
        "inventario.view_ajusteinventario",
        "inventario.add_ajusteinventario",
        "inventario.change_ajusteinventario",
    ],
    "PRODUCCION": [
        "recetas.view_receta",
        "recetas.view_lineareceta",
        "recetas.view_planproduccion",
        "recetas.add_planproduccion",
        "recetas.change_planproduccion",
        "recetas.view_planproduccionitem",
        "recetas.add_planproduccionitem",
        "recetas.change_planproduccionitem",
    ],
    "VENTAS": [
        "recetas.view_receta",
        "recetas.view_lineareceta",
        "crm.view_cliente",
        "crm.add_cliente",
        "crm.change_cliente",
        "crm.view_pedidocliente",
        "crm.add_pedidocliente",
        "crm.change_pedidocliente",
        "crm.view_seguimientopedido",
        "crm.add_seguimientopedido",
        "crm.change_seguimientopedido",
    ],
    "LOGISTICA": [
        "recetas.view_receta",
        "recetas.view_lineareceta",
    ],
    "RRHH": [
        "rrhh.view_empleado",
        "rrhh.add_empleado",
        "rrhh.change_empleado",
        "rrhh.view_nominaperiodo",
        "rrhh.add_nominaperiodo",
        "rrhh.change_nominaperiodo",
        "rrhh.view_nominalinea",
        "rrhh.add_nominalinea",
        "rrhh.change_nominalinea",
    ],
    "LECTURA": [
        "recetas.view_receta",
        "recetas.view_lineareceta",
        "recetas.view_planproduccion",
        "recetas.view_planproduccionitem",
        "maestros.view_insumo",
        "maestros.view_costoinsumo",
        "crm.view_cliente",
        "crm.view_pedidocliente",
        "crm.view_seguimientopedido",
        "rrhh.view_empleado",
        "rrhh.view_nominaperiodo",
        "rrhh.view_nominalinea",
    ],
}

class Command(BaseCommand):
    help = "Crea grupos/roles y asigna permisos básicos para Sprint 1."

    def handle(self, *args, **options):
        created = 0
        for role, perm_codes in ROLE_PERMS.items():
            group, was_created = Group.objects.get_or_create(name=role)
            if was_created:
                created += 1
            perms = []
            for code in perm_codes:
                try:
                    app_label, codename = code.split(".", 1)
                    perm = Permission.objects.get(content_type__app_label=app_label, codename=codename)
                    perms.append(perm)
                except Exception:
                    self.stdout.write(self.style.WARNING(f"Permiso no encontrado: {code}"))
            group.permissions.set(perms)
            group.save()
        self.stdout.write(self.style.SUCCESS(f"Roles listos. Nuevos grupos creados: {created}"))
