# Métricas de la vista BI

`reportes.views.bi` emite una línea JSON en el logger existente `erp.performance`
al terminar cada invocación autenticada, incluso si falla. No habilita el logger
SQL global ni cambia configuración, encabezados, cachés o respuestas.

La línea permite preguntar cuánto tardó la vista, cuánto de ese tiempo pasó en
ejecuciones SQL y si terminó con respuesta o excepción. Campos permitidos:

| Campo | Significado |
| --- | --- |
| `event` / `view` | Constantes `bi_request_metrics` / `reportes.bi` |
| `variant` | `portada` o `detalle`, según las condiciones existentes; `unknown` si la consulta no puede interpretarse tras una excepción |
| `total_ms` | Tiempo monotónico de la vista completa, incluido `render` |
| `sql_ms` / `query_count` | Suma y cantidad de `execute`/`executemany` en las conexiones Django durante esa invocación; incluye ejecuciones fallidas |
| `status` / `failed` | Estado HTTP de la respuesta y si es >=400; ante excepción, estado `null` y fallo `true` |
| `cf_ray_id` | Solo 16 caracteres hexadecimales en minúscula de un CF-Ray válido; `null` si falta o es inválido |

No registra SQL, parámetros, excepción ni su texto, usuario, cookies, IP,
credenciales, filtros, URL ni querystring. El contador usa `execute_wrapper`,
no agrega consultas y se retira al salir, también ante errores. Si el sink de
logging falla, se conserva la respuesta o excepción original; puede perderse
esa línea de métricas.

La autenticación exterior conserva su redirección existente: una petición que
no llega a la vista por falta de sesión no genera esta línea. Las excepciones
de permiso dentro de la vista se propagan; el estado HTTP que luego asigne el
middleware queda fuera de esta medición.

## Correlación y límites

Cloudflare envía CF-Ray al origen y en la respuesta al cliente. La documentación
advierte que el sufijo del centro de datos puede variar con Argo. Se acepta el
ID hexadecimal solo, o seguido por guion y tres letras mayúsculas; se descarta
el sufijo. La cabecera completa nunca se escribe. El ID es una pista de
correlación junto con la marca temporal del sink (por ejemplo, Docker logs con
timestamps) y la vista, no una prueba universal de unicidad ni una
credencial. [Fuente primaria: Cloudflare HTTP headers](https://developers.cloudflare.com/fundamentals/reference/http-headers/#cf-ray).

`total_ms` no incluye cola previa a la vista, autenticación/middleware exterior,
transferencia HTTPS, ejecución de JavaScript ni assets. `sql_ms` no mide apertura
de conexión, lectura posterior de filas ni commits que no pasen por el wrapper.
La diferencia entre total y SQL no es una medida de CPU. No demuestra caché de
aplicación/Redis/PostgreSQL fría ni explica retrospectivamente la portada de
55.178 s observada antes de instrumentar.

Para investigar otra demora, correlacionar una solicitud nueva con su respuesta
completa y esta línea. Conservar muestras distintas y sus condiciones; no
atribuir una demora a una consulta concreta con estas métricas agregadas.
