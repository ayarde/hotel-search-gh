# Hotel Search — Decisiones técnicas

**Estado:** decisiones de diseño acordadas
**Fecha de referencia de versiones:** 10-11-2026

## 1. Stack y estructura

- Group: `com.hotelsearch`. Artifact: `hotel-search`.
- Paquete base: `com.hotelsearch`.
- Java 25 LTS.
- Spring Boot 4.1.2 como versión estable de referencia, sujeto a
  verificación de compatibilidad de dependencias al implementar.
- Gradle 9.9.0 con Wrapper.
- Gradle Kotlin DSL (`build.gradle.kts`).
- Arquitectura hexagonal, en un único proyecto Gradle.
- Spring JDBC para persistencia.
- Spring Kafka para mensajería.
- JUnit 5, Mockito, JaCoCo y Testcontainers.
- Flyway para migraciones de Oracle.
- Docker y Docker Compose para ejecución local.
- Sin internacionalización de mensajes.

No usar Maven.

## 2. Arquitectura

### Domain (`com.hotelsearch.domain`)

Entidades, value objects e invariantes. No depende de Spring ni de
infraestructura.

### Application (`com.hotelsearch.application`)

Casos de uso y puertos: `SubmitHotelSearch`, `CountHotelSearch`,
`PersistHotelSearch`, `HotelSearchPublisher`, `HotelSearchRepository`.

### Infrastructure (`com.hotelsearch.infrastructure`)

Adaptadores: `input.rest`, `input.kafka`, `output.kafka`,
`output.persistence`, `config`.

Las dependencias apuntan hacia adentro. No crear interfaces sin una
frontera arquitectónica real.

## 3. Contrato REST

### POST `/search`

Recibe:

```json
{
  "hotelId": "550e8400-e29b-41d4-a716-446655440000",
  "checkIn": "09/11/2026",
  "checkOut": "29/11/2026",
  "ages": [29, 28, 4, 3]
}
```

- `hotelId`: UUID obligatorio, formato canónico de 36 caracteres, sin
  espacios; no hacer trim silencioso.
- `checkIn`, `checkOut`: formato `dd/MM/yyyy`; usar `LocalDate`
  internamente.
- `checkIn` debe ser anterior a `checkOut`.
- Ventana inclusiva para check-in: desde `today.minusMonths(1)` hasta
  `today.plusMonths(1)`, usando meses calendario.
- No se definió límite máximo para check-out, aparte de ser posterior
  a check-in.
- `ages`: obligatorio, no vacío, máximo 50 elementos; cada elemento no
  nulo y >= 0.
- El orden de edades forma parte de la identidad de la búsqueda.
- Tamaño máximo del body HTTP: 16 KB.
- Rechazar propiedades JSON desconocidas.
- Inyectar `Clock` para validar fechas de forma determinista en tests.
- Usar records y copias defensivas para inmutabilidad.

Comportamiento: 1. Validar request. 2. Generar `searchId` UUIDv4 con
`UUID.randomUUID()`. 3. Crear evento inmutable. 4. Publicar en Kafka
usando `searchId` como key. 5. Esperar ACK del broker. 6. Responder
**202 Accepted** con el `searchId` solo si el broker confirma.

El status 202 Accepted refleja que el POST confirma la aceptación del
evento por Kafka, no la persistencia inmediata en Oracle.

No hay idempotencia HTTP. Un retry del cliente puede generar otro
`searchId`.

Si Kafka no está disponible o falla la publicación, responder 503.

### GET `/count?searchId=...`

Devuelve: `searchId`; búsqueda completa; cantidad de otras búsquedas
con el mismo `hotelId`, `checkIn`, `checkOut` y lista `ages`
exactamente en el mismo orden.

La búsqueda consultada no se cuenta a sí misma. Si `searchId` no existe,
404.

### Errores

- 202 Accepted cuando el evento se publica correctamente en Kafka.
- 400 para validación, con mensaje útil.
- 404 para `searchId` inexistente.
- 503 para fallo de publicación Kafka.
- Preferir RFC 9457 Problem Details.

**Consistencia eventual:** POST confirma publicación en Kafka, no
persistencia en Oracle. El GET puede responder 404 durante el intervalo
entre ACK de Kafka y persistencia del consumer.

## 4. Kafka Producer

- Spring Kafka `KafkaTemplate`.
- Topic principal: `hotel_availability_searches`.
- `searchId` como message key.
- Esperar el ACK del broker antes de responder al POST.
- Configurar `acks=all`.
- Configurar `enable.idempotence=true`.
- Configurar `delivery.timeout.ms=10000` y `request.timeout.ms=3000`
  como valores iniciales revisables mediante pruebas.
- Reintentos nativos del producer; no agregar RetryTemplate ni
  `@Retryable`.
- Un broker y una partición inicialmente.

## 5. Kafka Consumer, reintentos y DLT

- Topic principal: `hotel_availability_searches`.
- Dead Letter Topic: `hotel_search_dlt`.
- Spring Kafka `DefaultErrorHandler`.
- Tres reintentos adicionales con intervalo fijo de 1 segundo.
- Errores permanentes van directamente al DLT.
- Errores transitorios agotados van al DLT.
- `DeadLetterPublishingRecoverer` para publicar al DLT.
- Si falla la publicación al DLT, no confirmar el offset original.
- No implementar consumidor ni reprocesamiento automático del DLT.
- Validar estructura básica del evento en el consumer.
- Configurar explícitamente los topics.

## 6. ACK, transacciones y duplicados

- Listener Kafka procesa el evento y retorna normalmente solo después
  de confirmar la transacción Oracle.
- `AckMode.RECORD`; ACK administrado por Spring Kafka.
- `@Transactional` en el servicio de aplicación que persiste.
- Commit Oracle antes de confirmar offset Kafka.
- Entrega at-least-once; no exactly-once.
- Sin XA ni transacción distribuida Kafka-Oracle.
- `search_id` es PK de Oracle.
- Insertar directamente. Una violación de la PK se trata como
  duplicado y se descarta.
- No hacer SELECT previo al INSERT.
- La PK es la única constraint unique prevista. Otros errores SQL se
  propagan.

## 7. Oracle

```sql
CREATE TABLE hotel_search (
    search_id   VARCHAR2(36 CHAR) NOT NULL,
    hotel_id    VARCHAR2(36 CHAR) NOT NULL,
    check_in    DATE NOT NULL,
    check_out   DATE NOT NULL,
    ages        VARCHAR2(600 CHAR) NOT NULL,
    created_at  TIMESTAMP(6) NOT NULL,
    CONSTRAINT pk_hotel_search PRIMARY KEY (search_id),
    CONSTRAINT ck_hotel_search_dates CHECK (check_in < check_out)
);
```

- Todos los nombres SQL en minúsculas.
- `ages` guarda JSON compacto, por ejemplo `[29,28,4,3]`, preservando
  orden.
- No agregar tabla de contadores.

Índice adoptado (opción segura):

```sql
CREATE INDEX idx_hotel_search_match
    ON hotel_search (
        hotel_id,
        check_in,
        check_out
    );
```

Se descarta incluir `ages` en el índice porque puede superar el límite
de longitud de clave. La igualdad de `ages` se resuelve en el predicado
de la consulta.

Query de consulta (una sola sentencia, bind parameters):

```sql
SELECT
    s.search_id,
    s.hotel_id,
    s.check_in,
    s.check_out,
    s.ages,
    s.created_at,
    (
        SELECT COUNT(*)
        FROM hotel_search other
        WHERE other.hotel_id = s.hotel_id
          AND other.check_in = s.check_in
          AND other.check_out = s.check_out
          AND other.ages = s.ages
          AND other.search_id <> s.search_id
    ) AS search_count
FROM hotel_search s
WHERE s.search_id = :searchId;
```

## 8. Transacciones Oracle

- Consumer: servicio de aplicación `@Transactional`.
- GET: `@Transactional(readOnly=true)` es aceptable.
- Un `DataSource` y un transaction manager.
- Nunca mantener transacción Oracle abierta esperando Kafka.
- ACK Kafka posterior al commit Oracle.

## 9. Virtual Threads y conexiones

```yaml
spring:
  threads:
    virtual:
      enabled: true
```

- Virtual Threads habilitados desde el inicio.
- HikariCP como pool JDBC.
- Máximo inicial de 10 conexiones, configurable.
- Kafka Consumer conserva los threads administrados por Kafka.
- Virtual Threads no aumentan la capacidad de Oracle.

La persistencia de las búsquedas en Oracle se ejecuta de forma síncrona
dentro del listener de Kafka. Spring Kafka gestiona el hilo y el ciclo de
ejecución en el que se invocan los listeners.

Delegar la persistencia a un executor independiente obligaría a esperar
explícitamente el resultado de la tarea y a coordinar correctamente sus
excepciones. Si el listener no esperara ese resultado, podría confirmar el
offset antes del commit de Oracle. Esta coordinación adicional no aporta una
mejora de rendimiento demostrada para el escenario actual.

La ejecución síncrona preserva el orden requerido por el diseño: el servicio
transaccional completa el commit en Oracle y, solo después, el listener
retorna. Spring Kafka puede entonces confirmar el offset correspondiente.

## 10. Logging y correlación (Micrometer Tracing ligero)

### Stack

- API: SLF4J
- Implementación: Logback (default de Spring Boot)
- Formato: JSON estructurado nativo de Spring Boot 4
- Correlación: MDC + Micrometer Tracing ligero

### Micrometer Tracing ligero

- Activar Micrometer Tracing (bridge OpenTelemetry o Brave).
- Habilitar observation en `KafkaTemplate` y en el listener container.
- Usar solo para enriquecer logs con `traceId` y `spanId`.
- No configurar exportador OTLP, Zipkin ni Jaeger en este challenge.

### Campos relevantes

- `timestamp`, `level`, `logger`, `message`
- `searchId` (cuando exista)
- `requestId` (opcional, capa REST)
- `traceId` / `spanId` (Micrometer Tracing)
- `component` (`rest`, `kafka-producer`, `kafka-consumer`, `persistence`)

### Propagación

1. El request HTTP coloca identificadores de correlación y `searchId`
   en el MDC.
2. El producer incluye `searchId` y el contexto de tracing como headers
   Kafka.
3. El consumer recupera los headers, los pone en el MDC y procesa el
   mensaje.
4. El MDC se limpia al finalizar cada ejecución (obligatorio con
   Virtual Threads).

### Restricciones

- No registrar payload completo ni lista de edades en producción.
- No registrar credenciales ni información sensible.
- Nivel root `INFO`; `DEBUG` solo en desarrollo y de forma acotada.

## 11. Testing y Gradle

- Tests unitarios: JUnit 5 + Mockito. Ubicación: `src/test/java`.
- Tests de integración: Testcontainers con Kafka y Oracle reales.
  Ubicación: `src/intTest/java`.
- Aplicar estrictamente las reglas de `TESTING_RULES.md`.
- `./gradlew build`: unit tests.
- `./gradlew build -PintegrationTest=true`: unit + integration.
- JaCoCo mínimo 80% en branches y líneas.

## 12. Docker y Docker Compose

Servicios: `app` (Spring Boot), `kafka` (Apache Kafka 4.4.0, KRaft),
`oracle` (Oracle AI Database 26ai Free).

Imágenes de referencia:
- Kafka: `apache/kafka:4.4.0`
- Oracle: `container-registry.oracle.com/database/free:23.26.3.0-lite`

Topics: `hotel_availability_searches` y `hotel_search_dlt`, una
partición cada uno.

Dockerfile multi-stage: builder con JDK 25 y Gradle Wrapper; runtime
JRE 25, usuario no root; copiar solo el Spring Boot executable JAR.

Compose: health checks; `depends_on` con `condition: service_healthy`;
volúmenes nombrados; `.env.example` versionado y `.env` ignorado;
`docker compose up --build` como comando principal.

## 13. Elementos expresamente fuera de alcance

- Despliegue cloud, Kubernetes o alta disponibilidad.
- Kafka multi-broker o particionado horizontal.
- Autenticación/autorización no requerida por el challenge.
- Exactly-once o atomicidad distribuida.
- Idempotencia HTTP.
- Outbox y publisher recovery.
- Administración/reprocesamiento de DLT.
- Publicación del artefacto en Maven Central u otro registry.
- Internacionalización de mensajes.
- Backend de tracing (Jaeger, Zipkin, OTLP exporter) en el challenge.
