# Fábrica de software del ERP — piloto

La fábrica conserva aceptación, autorización y evidencia por revisión dentro del
registro técnico existente. No crea tablas Django, no llama modelos, no ejecuta
un daemon y no concede permisos. Codex conduce la tarea y ejecuta cada paso
autorizado; `resume` informa el próximo paso, no inicia trabajo en segundo plano.

## Alcance y fuentes

- Crear y cerrar worktrees con `scripts/task_workspace_*`, como antes.
- Inscribir explícitamente las tareas del piloto. Los registros de tareas ajenas
  o anteriores no se migran ni se bloquean automáticamente.
- Fuente del ciclo de vida: `<git-common-dir>/task-workspaces/<estado>/<tarea>.json`.
- Evidencia: `task-workspaces/factory/<tarea>.json` y logs en `factory/<tarea>/`.
  Sobrevive al cierre del worktree y al cambio de chat; no se sincroniza a otros Macs.
- Los archivos llevan aceptación, referencia de autorización, comandos, resultados,
  SHA, tiempos, PR y commit integrado. No guardar credenciales, datos de clientes
  ni salidas sensibles; usar entorno seguro y comandos que no impriman secretos.

## Inicio

Después del inicio y preflight oficiales, desde el worktree:

```bash
python3 scripts/task_factory.py --repo "$PWD" --task NOMBRE init \
  --acceptance 'Comportamiento observable que debe cumplirse' \
  --authorization 'Referencia a la solicitud y límites autorizados' \
  --delivery production
```

`production` exige despliegue y verificación de versión en ejecución. `tooling`
se reserva a cambios consumidos en herramientas locales/CI/documentación, que no
requieren publicar la aplicación. Elegir según el diff, no para eludir un deploy.
La inscripción no se sobrescribe: una ampliación de autorización o cambio de modo
requiere detenerse, conservar evidencia y definir una nueva tarea autorizada.

## Recorrido

1. **local**: checks Django/migraciones y pruebas afectadas, con PostgreSQL aislado.
2. **review**: revisión completa del diff y comprobaciones pertinentes. El agente
   registra qué revisó; un comando exitoso no reemplaza su juicio sobre el código.
3. **ci**: lectura de GitHub; exige `test` exitoso del HEAD actual, base `main`,
   sin checks fallidos/pendientes y repositorio coincidente con `origin`.
4. **integration**: observa PR mergeado, revalida CI y comprueba que el commit
   integrado pertenece a `origin/main`. No hace merge por sí solo.
5. **deploy**, solo producción: ejecución del procedimiento oficial autorizado.
6. **verify**: ejecución de la aceptación en el consumidor real. En producción,
   la salida debe contener el SHA completo integrado leído del servicio en ejecución.
7. **cleanup**: ejecutar y comprobar limpieza de recursos propios; conservar logs.
8. **close**: el script oficial revalida GitHub y exige toda la evidencia antes
   de retirar ramas/worktree. La evidencia se conserva fuera del worktree.

Los pasos con comandos se ejecutan así, siempre sobre un commit limpio:

```bash
python3 scripts/task_factory.py --repo "$PWD" --task NOMBRE run \
  --stage local --timeout 600 --note 'Prueba y criterio observado' -- COMANDO ARGUMENTOS
python3 scripts/task_factory.py --repo "$PWD" --task NOMBRE ci --pr NUMERO
python3 scripts/task_factory.py --repo "$PWD" --task NOMBRE integration
python3 scripts/task_factory.py --repo "$PWD" --task NOMBRE resume
```

Los mismos argumentos sirven para `review`, `deploy`, `verify` y `cleanup`.
Para `deploy` y `verify` productivos se exige `--revision SHA_INTEGRADO`.
La verificación productiva debe leer la identidad del proceso/artefacto que sirve
la aplicación y probar la aceptación. `echo SHA`, un `git log` del VPS o un health
check aislado no prueban eso. No simular estas comprobaciones. La CLI valida el
contrato y conserva la salida; la calidad del comando sigue bajo responsabilidad
del operador. No es una firma de despliegue ni una frontera contra un agente hostil.

Los comandos se pasan como argumentos, sin shell implícito. Si se requiere shell,
usar un script revisado. No pasar instrucciones de issues o logs como comandos.
No adjuntar claves al comando: quedan registradas literalmente.

## Reanudación y límites

`status` y `resume` son consultas sin escritura. Reportan tareas sucias, ejecución
activa, pasos completos y siguiente paso. Un commit nuevo exige evidencia nueva.
Un comando que altera archivos o HEAD no cuenta como prueba exitosa. Cambiar o
borrar un log invalida el paso mediante su hash. Repetir un paso invalida los
posteriores. Ningún éxito anterior sustituye el último intento fallido.

Cada comando tiene timeout (600 s por defecto, máximo 3600) y tres intentos fallidos
por etapa/SHA. Se registra el intento antes de ejecutarlo; un cierre inesperado
queda como `running`, nunca aprobado. Al superar el límite, diagnosticar: no crear
commits vacíos para reiniciar el contador. El operador debe corregir la causa o
entregar el bloqueo por el flujo existente.

Un bloqueo `factory-<tarea>.lock` protege ejecuciones de la misma tarea sin mantener
el bloqueo global durante las pruebas. Antes de recuperar un lock tras una caída,
confirmar PID, propietario y ausencia de hijos en ejecución. No borrarlo por edad.
El cierre oficial comprueba el lock; una entrega/bloqueo del ciclo de vida debe
esperar a que termine el comando. Si el registro está entregado/bloqueado, `resume`
lo muestra, pero no avanza hasta recuperar formalmente propiedad activa.

Los comandos `deploy`/`verify` productivos usan además `factory-production.lock`,
compartido por las tareas inscritas del mismo repositorio. No permite despliegues
o verificaciones simultáneos entre ellas. No coordina otros repositorios ni
operadores que ejecuten el deploy fuera de la fábrica. Entre pasos se revalida la
versión real; el lock no es un permiso para publicar una revisión distinta.

## Recursos y cierre

Registrar proyecto Compose, contenedores, volúmenes, puertos y propietario antes
de crearlos. No usar producción como entorno de prueba. El paso `cleanup` debe
comprobar ausencias con nombres exactos o referir una entrega explícita con
responsable, motivo y fecha; la nota por sí sola no sustituye el inventario.

Si la limpieza necesita un registro terminal para el helper de Docker, conservar
los recursos entregados a un responsable con fecha, cerrar Git por el flujo
oficial y ejecutar el helper obligatorio. La entrega final debe aclarar recursos
pendientes hasta confirmar su retiro. No confundir cierre Git con recursos retirados.
No eliminar registros/logs de la fábrica al limpiar el worktree.

## Comprobación y medición

```bash
python3 -m unittest discover -s scripts/tests -p test_task_factory.py
bash scripts/tests/test_task_workspace_lifecycle.sh
```

Ambas comprobaciones forman parte del job `checks` del CI bloqueante. Los tests
usan repositorios descartables y respuestas GitHub simuladas en el límite de API;
no son pruebas de producción. El piloto real usa GitHub y el registro de la tarea.

Comparar cinco tareas pequeñas equivalentes: tiempo hasta entrega verificada,
preparación, ejecución local, cola/ejecución CI, revisión, despliegue, reanudación,
intentos fallidos e intervenciones de Mauricio. Los eventos capturan tiempo de los
comandos; los tiempos de cola CI y la intervención humana se recogen aparte.
Esta versión no calcula ahorro ni costo monetario. No promete autonomía 24/7,
orquestación de varios agentes, auditoría de seguridad o un panel web.
