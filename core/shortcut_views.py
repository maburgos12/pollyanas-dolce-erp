from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect

from core.access import can_view_module, can_view_submodule, is_bonos_produccion_capture_only


@login_required
def shortcut_bonos_produccion(request):
    if is_bonos_produccion_capture_only(request.user) or can_view_submodule(request.user, "produccion", "bonos"):
        return redirect("/bonos-produccion/app/?captura=1")
    if can_view_module(request.user, "seguimiento"):
        return redirect("/seguimiento/")
    return redirect("/dashboard/")


@login_required
def shortcut_bonos_ventas(request):
    if can_view_submodule(request.user, "ventas", "bonos"):
        return redirect("/bonos-ventas/app/?captura=1")
    if can_view_module(request.user, "seguimiento"):
        return redirect("/seguimiento/")
    return redirect("/dashboard/")
