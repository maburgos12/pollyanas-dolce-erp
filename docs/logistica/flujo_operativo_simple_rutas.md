# Flujo operativo simple de rutas

Esta es la version usable del flujo. El diagrama grande queda como inventario tecnico.

## Flujo correcto

1. La sucursal solicita para surtir al dia siguiente.
2. CEDIS consolida lo que se debe surtir.
3. Logistica arma la ruta con sucursales y, si aplica, una parada CEDIS para recarga.
4. `Actualizar carga esperada` toma lo que realmente se va a enviar para el tramo actual.
5. La PWA muestra solo el tramo actual, no toda la ruta mezclada.
6. El repartidor confirma la carga en PWA.
7. Logistica o PWA da salida y la ruta pasa a `EN_RUTA`.
8. GPS puede marcar llegada/visitada solo con orden y permanencia.
9. Entrega se marca aparte, con productos/evidencia.
10. Si toca CEDIS, se registra recarga y se abre el siguiente tramo.
11. Point recibido se sincroniza cuando la sucursal recibe/cierra.
12. La ruta se cierra solo cuando no quedan pendientes o cuando logistica autoriza una diferencia.

## Reglas que evitan los bugs vistos

- `VISITADA` no significa `ENTREGADA`.
- `ENVIADO` en Point no significa `RECIBIDO`.
- `CEDIS` no es entrega; es recarga o separador de tramo.
- Carga esperada, carga marcada y recepcion final son tres evidencias distintas.
- La PWA no debe mostrar productos de tramos futuros antes de llegar a CEDIS.
- Si hay diferencia de carga o entrega, debe existir motivo.
- Si la PWA no cambia, revisar service worker/cache antes de culpar la logica.
