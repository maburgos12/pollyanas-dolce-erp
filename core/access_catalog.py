"""Opciones persistibles derivadas del catálogo canónico de accesos."""

from core.access import ACCESS_MODULES, ACCESS_SUBMODULES


def build_user_module_access_choices() -> list[tuple[str, str]]:
    """Aplana módulos y pestañas para el campo ``UserModuleAccess.module``."""
    choices: list[tuple[str, str]] = []
    for module, module_label in ACCESS_MODULES:
        choices.append((module, module_label))
        choices.extend(
            (f"{module}.{submodule}", f"{module_label} - {submodule_label}")
            for submodule, submodule_label in ACCESS_SUBMODULES.get(module, [])
        )
    return choices


USER_MODULE_ACCESS_CHOICES = build_user_module_access_choices()
