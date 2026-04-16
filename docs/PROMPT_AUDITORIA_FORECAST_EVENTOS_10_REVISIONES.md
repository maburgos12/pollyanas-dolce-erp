# Prompt operativo de cierre para forecast estacional

Usa este prompt cuando se recalculen o validen eventos estacionales del ERP y se necesite una evidencia seria antes de decir que el forecast quedó aplicado.

## Prompt

Trabaja dentro de este repositorio como agente senior de forecasting ejecutivo y control ERP.

Tu tarea es ejecutar una auditoría cerrada de forecast estacional sobre `Día del Niño`, `Día de las madres` y `Día del Padre`, usando el loop oficial del repo y sin declarar nada como corregido si no quedó persistido y verificado.

Reglas no negociables:
- el ingreso siempre es `precio real vigente por SKU x piezas forecast por SKU`
- no excluyas productos reales del evento solo por venderse en vaso; `Vasos Preparados` cuentan cuando son postre real del evento
- sí excluye accesorios, bebidas de reventa y `modo_costeo=SERVICIO_ACCESORIO`
- si el recálculo no persiste o se cuelga, reporta bloqueo real y conserva los últimos valores verificados
- no digas "aplicado" si el ERP no muestra valores persistidos nuevos

Debes correr la auditoría oficial:

```bash
python manage.py audit_seasonal_event_forecasts --enforce-status --write-report
```

La auditoría debe cubrir estas 10 revisiones por evento:
1. forecast persistido
2. snapshot persistido
3. financial BASE persistido
4. modelo ejecutivo trazable
5. scope comercial limpio
6. semana sin bandera de plausibilidad
7. semana dentro del techo ejecutivo
8. día principal arriba del piso del homólogo
9. estado consistente con el guard
10. comparativa diaria completa contra 2025

Y además debe entregar:
- comparativa diaria 2026 vs 2025 por cada día del rango auditado
- top productos de la semana 2026 vs 2025
- artefactos vigentes de la versión auditada
- decisión go/no-go por evento

Salida esperada:
- reporte markdown en `output/forecast_audits/`
- resumen claro de qué pasó en `Niño`, `Madres` y `Padre`
- ningún evento se considera cerrado si todavía falla una de las 10 revisiones

## Comando oficial

```bash
./.venv/bin/python manage.py audit_seasonal_event_forecasts --enforce-status --write-report
```
