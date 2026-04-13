# Loop de respuesta ejecutiva DG

Usar este loop cuando Direccion General haga una pregunta ejecutiva.

## Paso 1. Clasificar la pregunta

Identificar si la pregunta busca:
- cifra auditada
- forecast operativo
- riesgo
- recomendacion de compra / produccion / revision

Si mezcla varias, responder las cuatro capas en una sola salida.

## Paso 2. Buscar hecho auditado

Extraer primero lo que si esta confirmado en ERP:
- compras registradas
- ordenes
- solicitudes
- inventario
- ventas
- publicaciones visibles
- KPI disponibles

Nunca empezar por la limitacion; empezar por el dato confirmado.

## Paso 3. Separar comprometido vs estimado

Declarar explicitamente:
- que esta comprometido hoy en ERP
- que es una estimacion operativa

Nunca mezclar ambas categorias.

## Paso 4. Intentar estimar

Si no existe forecast directo, construir un estimado usando proxies:
- ventas recientes
- consumo reciente
- inventario actual
- stock bajo
- recetas / BOM
- dias restantes
- compras pendientes

Si no alcanza para una cifra seria, decir exactamente que calculo falta.

## Paso 5. Traducir a decision

Convertir los datos a lectura de negocio:
- riesgo de faltante
- riesgo de subcaptura
- riesgo de sobrecompra
- necesidad de revisar compras
- necesidad de recalcular con mas variables

## Paso 6. Cerrar con accion concreta

La salida debe terminar con una accion ejecutable, por ejemplo:
- recalcular forecast
- revisar solicitudes pendientes
- registrar compras comprometidas
- contrastar consumo e inventario

## Formato final

Siempre responder en este orden:

1. `Resumen ejecutivo`
2. `Hecho auditado`
3. `Estimado operativo`
4. `Riesgo / interpretacion`
5. `Siguiente accion recomendada`

