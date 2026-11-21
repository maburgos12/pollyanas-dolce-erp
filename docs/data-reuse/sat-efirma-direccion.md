# Ficha de fuentes — e.firma SAT desde Dirección

Fecha: 2026-11-21. Base de código 0f2eb656; PostgreSQL 16 aislado con datos sintéticos.

Necesidad: renovar la credencial de la misma empresa para descarga masiva, sin duplicar solicitudes, CFDI ni reglas fiscales.

| Concepto | Fuente y clave | Evidencia | Consumidores |
| --- | --- | --- | --- |
| Credenciales actuales | SAT_EFIRMA_CER_PATH, SAT_EFIRMA_KEY_PATH, SAT_EFIRMA_PASSWORD y SAT_RFC en configuración de servidor | Lectura get_sat_credentials; inventario_fuentes_datos --term certificado --term efirma no encontró modelo en la base inicial aislada | Firma SOAP, autenticación, solicitud, verificación y descarga |
| Sustitución autorizada | ConfiguracionEfirma, clave única 1 y RFC fijo de servidor | Nuevo registro cifrado; pruebas sintéticas de activación, rechazo, cifrado y fallback de claves | Mismo get_sat_credentials en web y worker SAT; no caché de proceso |
| Solicitudes y CFDI | SolicitudDescarga.id_solicitud y CfdiDescargado.uuid | Modelos existentes; no se escriben desde este formulario | Tarea nocturna y conciliación |
| Evidencia de actualización/prueba | LogDescargaSat; actor e instante de ConfiguracionEfirma | Mensajes fijos sin contraseña, token ni llave; huella pública de certificado para pruebas del entorno legado | Panel SAT y auditoría operativa |

Equivalencias: .cer/.key/contraseña se validan como un par criptográfico; el RFC del sujeto debe coincidir exactamente con SAT_RFC. No se crean alias ni se permite cambiar la identidad fiscal. La autenticación remota es distinta de vigencia local, revocación, recepción de paquetes y cobertura de CFDI.

Decisión: extender el proveedor existente. Antes de la primera activación se conserva el origen por entorno. Después, el proveedor descifra el registro único en memoria; no escribe archivos en media ni en temporales. Una configuración cifrada ilegible falla explícitamente, sin volver silenciosamente al certificado anterior.

Validación: inventario automatizado de metadatos sobre PostgreSQL de la tarea; grafo de consumidores contrastado con fuente vigente. No se inspeccionaron valores fiscales productivos para esta ampliación. Las pruebas SAT locales utilizan un adaptador sintético; la autenticación real de la nueva e.firma requiere su captura por Dirección en producción.

Recuperación: respaldar la base y conservar SECRET_KEY/SECRET_KEY_FALLBACKS por el canal seguro existente. Si se rota SECRET_KEY, mantener la anterior como fallback hasta reactivar/re-cifrar la e.firma; no publicar estas claves en tickets ni PR.
