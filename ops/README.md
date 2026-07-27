# ops/ — servicios de operacion fuera del proyecto Django

Codigo que corre en el VPS **junto** al ERP pero no forma parte del proyecto Django: agentes,
integraciones y jobs con su propio entorno y sus propias unidades `systemd`. Django no importa nada de
esta carpeta.

Vive aqui para tener historial y revision — antes estaba suelto en `/opt` sin control de versiones.
Cada subcarpeta documenta su ruta de despliegue y como publicar cambios.

| Carpeta | Que hace | Despliegue |
|---|---|---|
| `agente-hikconnect/` | Trae las checadas desde Hik-Connect Cloud al receptor del ERP, mas su catch-up diario | `/opt/agente-hikconnect` |

**Ningun secreto se versiona aqui.** Los `.env`, sesiones y bases de estado viven solo en el servidor;
cada subcarpeta trae su `.env.example`.

Para scripts del propio proyecto Django (deploy, backups, mantenimiento) ver `scripts/`.
