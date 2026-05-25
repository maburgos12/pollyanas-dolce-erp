# Checklist De Revision Visual

Usar en PRs que agreguen o modifiquen UI.

## Reglas De Sistema

- [ ] Usa `base.html` o el shell PWA correspondiente.
- [ ] No agrega `style=""` nuevo salvo print/correo/PDF justificado.
- [ ] No agrega bloque `<style>` nuevo en template.
- [ ] No agrega colores hex/RGB/HSL sueltos en templates.
- [ ] Usa tokens de `static/css/pollyana_ops_ui.css`.
- [ ] No usa emojis como iconos funcionales.
- [ ] No usa `transition: all`.
- [ ] No usa cards dentro de cards para decorar.

## Operacion

- [ ] La accion principal esta visible arriba.
- [ ] Los filtros esenciales no tapan el trabajo principal.
- [ ] Las fuentes de datos/fechas/cortes se ven cuando hay metricas.
- [ ] Existe estado vacio.
- [ ] Existe estado de error o bloqueo cuando aplica.
- [ ] Existe estado sin permisos cuando aplica.

## Componentes

- [ ] Botones con default, hover, focus-visible, active y disabled.
- [ ] Inputs con foco visible sin cambiar ancho de borde.
- [ ] Tablas con numeros tabulares.
- [ ] Badges usan tonos del sistema.
- [ ] Modales tienen accion primaria, cancelar y cierre accesible.

## Responsive

- [ ] Sin scroll horizontal de pagina en 320 px.
- [ ] Tablas usan contenedor con scroll horizontal visible.
- [ ] Botones, tabs y links no parten texto en dos lineas.
- [ ] Validado en 320, 375, 414 y 768 px.

## Validacion

- [ ] `python manage.py check` pasa.
- [ ] `python manage.py migrate --check` si se va a deploy.
- [ ] `collectstatic --noinput` considerado si se agregan estaticos.
- [ ] Navegador real validado si afecta flujo visible.
- [ ] Captura o evidencia concreta adjunta al cierre.
