# Hotel Search — Requerimientos

## 1. Objetivo

Construir una aplicación Java que reciba búsquedas de hoteles, publique
cada búsqueda en Kafka, la persista asincrónicamente en Oracle y permita
consultar la búsqueda junto con la cantidad de búsquedas equivalentes.

## 2. Requerimientos funcionales

### RF-01 — Registrar una búsqueda

Exponer `POST /search`.

Request de ejemplo:

```json
{
  "hotelId": "550e8400-e29b-41d4-a716-446655440000",
  "checkIn": "09/11/2026",
  "checkOut": "29/11/2026",
  "ages": [29, 28, 4, 3]
}
```

El endpoint debe: 1. validar el payload; 2. generar un `searchId` único;
3. construir un evento inmutable; 4. publicar el evento en el topic
Kafka; 5. esperar confirmación del broker; 6. responder **202 Accepted**
con el `searchId`.

El status 202 Accepted indica que Kafka aceptó el evento; la
persistencia en Oracle es asíncrona.

Cada solicitud válida genera un ID nuevo, incluso si el payload es
idéntico a otro.

No se requiere idempotencia HTTP.

### RF-02 — Consultar una búsqueda y contar equivalentes

Exponer `GET /count?searchId={uuid}`.

La respuesta debe incluir: `searchId`; búsqueda completa (`hotelId`,
`checkIn`, `checkOut`, `ages`); cantidad de otras búsquedas que
coinciden exactamente con los campos de búsqueda.

La búsqueda cuyo ID se consulta no se incluye en el conteo.

Si el ID no existe en Oracle, responder 404.

### RF-03 — Consumir y persistir eventos

Un consumer Kafka debe: consumir eventos del topic
`hotel_availability_searches`; validar estructura básica del evento;
persistirlos en Oracle; confirmar el offset después del commit de
Oracle; enviar a DLT los mensajes no procesables según la política
definida.

## 3. Reglas de validación

### Identificadores

- `hotelId` obligatorio.
- Debe ser UUID válido, canónico, de 36 caracteres.
- No se permiten espacios.
- No normalizar mediante trim silencioso.
- `searchId` generado por el servidor como UUIDv4.

### Fechas

- Formato HTTP: `dd/MM/yyyy`.
- Tipo interno: `LocalDate`.
- `checkIn` anterior a `checkOut`.
- `checkIn` dentro de la ventana inclusiva `today - 1 mes calendario`
  a `today + 1 mes calendario`.
- `checkOut` no tiene límite superior adicional, pero debe ser
  posterior a `checkIn`.
- Usar `Clock` inyectable en validaciones.

### Edades

- `ages` obligatorio y no vacío.
- Máximo 50 elementos.
- Ningún elemento puede ser null.
- Cada edad debe ser >= 0.
- El orden de la lista importa para la equivalencia.

### JSON y HTTP

- Body máximo: 16 KB.
- Rechazar propiedades JSON desconocidas.
- Campos obligatorios no pueden ser nulos o vacíos.
- Errores de validación deben incluir un mensaje útil.

## 4. Requerimientos no funcionales

- Java 25 LTS o superior compatible con el baseline elegido.
- Spring Boot 3+; el diseño toma Spring Boot 4.1.2 como versión
  estable de referencia.
- Gradle Wrapper; Kotlin DSL.
- Arquitectura hexagonal en un único proyecto.
- Spring JDBC para Oracle.
- Kafka producer y consumer separados.
- Virtual Threads habilitados.
- Thread safety.
- SQL parametrizado para evitar SQL injection.
- Modelos/eventos inmutables.
- No usar `java.util.Date`; usar `LocalDate`.
- Evitar concatenación de String `+` dentro de loops.
- Tests JUnit y Mockito.
- Cobertura mínima JaCoCo 80% en branch y líneas.
- Swagger/OpenAPI.
- Docker y Docker Compose.
- La aplicación debe compilarse dentro de Docker.
- Docker Compose debe permitir levantar la aplicación y sus
  dependencias con un único comando principal.
- README con comandos de ejecución, Swagger y endpoints.
- Considerar rendimiento tanto en código como en tests.
- Usar `assertAll` cuando sea útil.
- Sin internacionalización de mensajes.

## 5. Contrato de errores

| Situación                                        | HTTP |
|--------------------------------------------------|------|
| Evento aceptado por Kafka                        | 202  |
| Request inválido                                 | 400  |
| `searchId` inexistente                           | 404  |
| Kafka no disponible / publicación no confirmada  | 503  |

Usar Problem Details (RFC 9457) como formato recomendado.

La disponibilidad de Kafka no implica que el consumer ya haya persistido
el evento. Debido al procesamiento asincrónico, puede existir una
ventana temporal donde POST ya respondió 202 y GET todavía responde 404.

## 6. Modelo de persistencia

Tabla `hotel_search`:

| Columna      | Tipo                | Restricción / significado        |
|--------------|---------------------|----------------------------------|
| `search_id`  | `VARCHAR2(36 CHAR)` | PK, UUID del servidor            |
| `hotel_id`   | `VARCHAR2(36 CHAR)` | UUID del hotel                   |
| `check_in`   | `DATE`              | Fecha de entrada, medianoche     |
| `check_out`  | `DATE`              | Fecha de salida, medianoche      |
| `ages`       | `VARCHAR2(600 CHAR)`| JSON compacto de edades, orden preservado |
| `created_at` | `TIMESTAMP(6)`      | Fecha/hora de creación           |

Constraint `check_in < check_out`.

El esquema se gestiona con Flyway. Los nombres SQL (tablas, columnas,
índices y constraints) deben escribirse en minúsculas.

## 7. Comportamiento de mensajería

- Topic principal: `hotel_availability_searches`.
- DLT: `hotel_search_dlt`.
- `searchId` es la key del mensaje.
- Un broker y una partición por topic en la primera versión.
- Producer espera ACK; `acks=all`, idempotencia nativa del producer
  activada.
- Consumer: tres reintentos adicionales, 1 segundo entre reintentos;
  luego DLT.
- Errores permanentes se envían directamente a DLT.
- Si no se puede publicar al DLT, no confirmar el offset original.
- ACK del mensaje original solo después de commit Oracle.
- Entrega at-least-once; duplicados resueltos mediante PK `search_id`.

## 8. Testing

### Unitarios

JUnit 5 + Mockito para dominio, casos de uso, validaciones, mapeos y
lógica aislada. Ubicación: `src/test/java`.

Aplicar `TESTING_RULES.md`.

### Integración

Testcontainers con instancias reales de Kafka y Oracle AI Database Free.
No utilizar H2 como sustituto de Oracle. Ubicación: `src/intTest/java`.

Aplicar `TESTING_RULES.md`.

Comandos:

```bash
./gradlew build
./gradlew build -PintegrationTest=true
```

## 9. Ejecución local

`docker compose up --build` debe: construir la imagen de la aplicación
usando Gradle Wrapper; levantar Kafka en KRaft; levantar Oracle; esperar
health checks; iniciar la aplicación cuando las dependencias estén
saludables.

Documentar: configuración `.env`; puertos; URL de Swagger; endpoints;
cómo detener servicios y cómo eliminar volúmenes.

## 10. Fuera de alcance

- Idempotencia HTTP.
- Outbox pattern.
- Publisher recovery, leases y scheduler.
- Exactly-once / transacción distribuida.
- Kafka multi-broker o HA.
- Reprocesamiento automático del DLT.
- Tabla contadora.
- Publicación del artefacto en Maven Central o cualquier registry.
- Kubernetes o despliegue cloud.
- Internacionalización de mensajes.
