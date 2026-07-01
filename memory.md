# CENACE — memoria de recuperación operativa

Fecha de inicio: 2026-07-01 (America/Guayaquil)

## Reglas de preservación

- No eliminar la carpeta afectada ni la copia `CENACE_mejorado_20260505`.
- Construir y probar la recuperación fuera de `CENACE` antes del corte.
- Conservar la carpeta afectada como `CENACE_incidente_20260630` durante el corte.
- No registrar contraseñas, tokens ni claves privadas en este documento o en Git.
- No reutilizar el token GitHub encontrado en la URL del remoto antiguo.

## Evidencia y fuentes seleccionadas

- Ventana del incidente: 2026-06-30 16:30:01–16:35:17 UTC-05:00.
- Informe forense: `findings_auditoria_CENACE_2026-07-01.txt`.
- GitHub sano: commit `d99624a2ee9c9744bf35caf69a775ca71e7850dd`, generado por la ejecución de las 15:30.
- Copia local histórica: `/home/srvdpahidrologia/CENACE_mejorado_20260505`.
- PostgreSQL es la fuente canónica de datos: caudales hasta 2026-06-30 15:30:01 y nivel hasta 15:00:02.
- FTP CENACE fue validado en lectura y contiene `caudales` y `niveles`.
- El usuario copió el 2026-07-01 el generador del boletín, su JSON y la plantilla DOCX.

Hashes del material de boletín recuperado:

- `generar_boletin_hidroelectricas.py`: `f249ded20008dfd9637497f8cabf21ab796560631f0d098ab7221fcafd3a78dc`
- `boletin_hidroelectricas_config.json`: `2b525fcd71ad7e959073ab028e809af1b84058cf225d4f0501ac726fd005ddde`
- `Caudal_Junio_26_2026.docx`: `9720629b39f9c45317ad93637d591bdb9d3365b43f151376718e22833921b8d1`

## Decisiones de recuperación

- Construcción en staging y cambio controlado; nunca restauración directa sobre evidencia.
- Base Git limpia desde el último commit sano; incorporar selectivamente archivos locales no versionados.
- Entorno virtual Python 3.10 nuevo y dependencias fijadas en `requirements.txt`.
- Boletín con análisis determinista, sin dependencia de OpenAI para cron.
- GitHub mediante clave SSH con escritura, sin token en el remoto.
- Credenciales PostgreSQL y FTP actuales se reutilizan temporalmente con `config.ini` en modo `0600`.
- Rotar credenciales PostgreSQL y FTP dentro de las 24 horas siguientes al corte.
- Cron se habilita únicamente tras pruebas completas de ingesta, salidas, boletín y Git.

## Interfaces operativas esperadas

- Ingesta horaria: `cenace.sh` al minuto 30.
- Generación/publicación horaria: `sync_if_new_hour.py` al minuto 35.
- Diario: datos a la 01:00, outliers 01:15, TAB2 01:20, TAB4 01:25 y push 01:30.
- Boletín diario: `run_boletin_hidroelectricas.sh` a las 11:55.
- Destino de boletín: recurso CIFS `Hidroelectricas_INFORME`.

## Puerta de producción

La recuperación solo puede sustituir a `CENACE` si:

1. Python y shell pasan validaciones sintácticas e importaciones.
2. PostgreSQL y FTP pasan preflight.
3. TAB1–TAB4 se generan con archivos válidos y no vacíos.
4. El boletín genera 13 figuras, narrativa determinista y un DOCX válido.
5. Git publica mediante SSH sin exponer secretos.
6. Cada comando funciona con un entorno equivalente a cron.
7. Existe un respaldo `pg_dump` verificable anterior a cualquier ingesta.

## Rollback

- Deshabilitar inmediatamente las tareas cron de CENACE.
- Apartar la versión fallida sin eliminarla.
- Conservar `CENACE_incidente_20260630`, el staging y el dump PostgreSQL.
- Corregir y repetir toda la puerta de producción antes de un nuevo corte.
