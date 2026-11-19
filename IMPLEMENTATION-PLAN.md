# Hotel Search — Plan de implementación

## Forma de trabajo

Implementar en fases pequeñas. **Antes de iniciar cada fase**, describir
alcance, criterios de aceptación, cambios previstos y validaciones.
**Al finalizar**, mostrar diff, resultados reales de pruebas y cualquier
desviación.

No avanzar a la siguiente fase si hay fallos en criterios obligatorios
de la fase actual.

Aplicar `TESTING_RULES.md` en todas las fases que generen tests.

------------------------------------------------------------------------

## Fase 0 — Bootstrap del proyecto

### Alcance

- Crear proyecto Spring Boot con Java 25.
- Group `com.hotelsearch`, artifact `hotel-search`.
- Gradle Wrapper y Kotlin DSL.
- Configurar repositorios, plugins y catálogo de versiones.
- Configurar aplicación ejecutable.
- Crear estructura inicial de paquetes hexagonales.

### Criterios de aceptación

- No existe `pom.xml`.
- `./gradlew tasks` funciona.
- La aplicación compila y arranca localmente.
- El código se organiza en un único proyecto Gradle.
- `domain` no depende de Spring.

### Validación

```bash
./gradlew clean build
```

------------------------------------------------------------------------

## Fase 1 — Dominio y casos de uso

### Alcance

- Modelos inmutables de búsqueda y evento.
- Value objects para identificadores y reglas.
- Validaciones de dominio: UUID, fechas, edades y orden de edades.
- Casos de uso `SubmitHotelSearch`, `CountHotelSearch` y
  `PersistHotelSearch`.
- Puertos `HotelSearchPublisher` y `HotelSearchRepository`.
- Inyectar `Clock` para reglas temporales.

### Criterios de aceptación

- Dominio sin dependencias de infraestructura.
- `searchId` generado por servidor como UUIDv4.
- Las listas son inmutables y conservan el orden.
- Check-in respeta ventana inclusiva de un mes calendario.
- Check-out es posterior a check-in.
- Edades: lista no vacía, hasta 50, sin null, valores >= 0.
- Dos búsquedas iguales pueden tener distintos `searchId`.

### Validación

- Unit tests para cada regla y límites (`TESTING_RULES.md`).
- Tests de inmutabilidad y defensive copy.
- Tests con `Clock` fijo.
- `./gradlew build`.

------------------------------------------------------------------------

## Fase 2 — Persistencia Oracle y migraciones

### Alcance

- Configurar Spring JDBC y HikariCP.
- Agregar Flyway y migración inicial de `hotel_search`.
- Implementar `HotelSearchRepository`.
- Consulta de búsqueda y count con una sola sentencia SQL.
- Inserción y manejo de duplicado por PK sin SELECT previo.
- Índice en `(hotel_id, check_in, check_out)`; igualdad de `ages` en
  predicado.

### Criterios de aceptación

- DDL cumple nombres SQL en minúsculas y modelo acordado.
- `ages` se guarda como JSON compacto, orden preservado.
- Consulta devuelve cero si no hay otras búsquedas iguales y excluye
  el `searchId` consultado.
- `searchId` inexistente se distingue de count cero.
- SQL usa bind parameters.
- Solo la violación de PK prevista se trata como duplicado.

### Validación

- Integration tests con Oracle Testcontainer real (`src/intTest`).
- Verificar INSERT, lectura, igualdad de edades, constraints, rollback
  y duplicado.
- `./gradlew build -PintegrationTest=true`.

------------------------------------------------------------------------

## Fase 3 — Kafka producer

### Alcance

- Configurar Spring Kafka producer.
- Topic `hotel_availability_searches`.
- Serialización JSON del evento.
- Usar `searchId` como message key.
- Configurar ACK, idempotencia del producer y timeouts.
- Implementar adaptador `HotelSearchPublisher`.

### Criterios de aceptación

- Evento publicado con `searchId` como key.
- POST/caso de uso no informa éxito antes del ACK.
- Fallo o timeout de publicación se traduce a error de disponibilidad.
- No se agrega idempotencia HTTP ni Outbox.
- No se abre transacción Oracle durante la publicación.

### Validación

- Integration test con Kafka Testcontainer.
- Verificar key, payload y ACK.
- `./gradlew build -PintegrationTest=true`.

------------------------------------------------------------------------

## Fase 4 — Kafka consumer, persistencia, reintentos y DLT

### Alcance

- Configurar listener para `hotel_availability_searches`.
- Configurar `AckMode.RECORD`.
- Implementar persistencia transaccional.
- Configurar `DefaultErrorHandler`, tres reintentos adicionales con 1
  segundo de intervalo.
- Errores permanentes al DLT directamente.
- Configurar `DeadLetterPublishingRecoverer` y topic `hotel_search_dlt`.
- Crear ambos topics explícitamente con una partición y replication
  factor 1.

### Criterios de aceptación

- Consumer valida estructura básica.
- Offset se confirma solo después del commit Oracle.
- Oracle falla: rollback y excepción propagada.
- Redelivery del mismo mensaje no crea una segunda fila.
- Duplicado PK se descarta sin ocultar otros errores SQL.
- Tras tres reintentos adicionales, error transitorio va al DLT.
- Error permanente va directamente al DLT.
- No hay consumidor automático del DLT.

### Validación

- Integration tests con Kafka y Oracle reales.
- Verificar persistencia, redelivery, rollback, reintentos y DLT.
- `./gradlew build -PintegrationTest=true`.

------------------------------------------------------------------------

## Fase 5 — API REST y contrato HTTP

### Alcance

- DTOs request/response.
- `POST /search` y `GET /count`.
- Validación HTTP y de dominio.
- Formato de fechas `dd/MM/yyyy`.
- Límite real de body 16 KB.
- Rechazo de propiedades JSON desconocidas.
- Problem Details.
- Swagger/OpenAPI.

### Criterios de aceptación

- Request válido publica y responde **202 Accepted** con `searchId`
  tras ACK Kafka.
- Request inválido responde 400 con mensaje útil.
- Kafka no disponible responde 503.
- GET devuelve búsqueda y count excluyendo la propia.
- GET para ID inexistente responde 404.
- El orden de `ages` cambia la equivalencia.
- Se documentan endpoints y esquemas en OpenAPI.
- Se documenta la ventana de consistencia eventual.

### Validación

- Controller tests e integration tests REST + Kafka + Oracle.
- Verificar body > 16 KB, campos desconocidos, fechas y UUID inválidos.
- `./gradlew build -PintegrationTest=true`.

------------------------------------------------------------------------

## Fase 6 — Virtual Threads, logging y configuración operativa

### Alcance

- Habilitar Virtual Threads.
- Configurar HikariCP (máx. 10 conexiones, externalizado).
- Externalizar bootstrap servers, JDBC URL, usuario, password y
  timeouts.
- Configurar logging estructurado JSON + MDC.
- Activar Micrometer Tracing ligero (solo logs y correlación).
- Habilitar observation en Kafka producer y consumer.
- Revisar shutdown.

### Criterios de aceptación

- Virtual Threads habilitados en runtime.
- Tamaño del pool JDBC configurable.
- No hay pools de threads propios innecesarios.
- Logs en JSON con `searchId`, `traceId` (si aplica) y `component`.
- MDC se limpia al finalizar cada operación.
- Observation habilitada en KafkaTemplate y listener.

### Validación

- Context test de configuración.
- Verificar campos de correlación en logs.
- `./gradlew build`.

------------------------------------------------------------------------

## Fase 7 — Docker, Docker Compose y Flyway end-to-end

### Alcance

- Dockerfile multi-stage con Gradle Wrapper/JDK 25 y runtime JRE 25.
- Docker Compose para app, Kafka KRaft y Oracle Free.
- Health checks y `depends_on: condition: service_healthy`.
- Listeners Kafka interno y externo.
- Volúmenes persistentes.
- Variables de entorno y `.env.example`; `.env` en `.gitignore`.
- Flyway ejecuta migraciones al arrancar.

### Criterios de aceptación

- `docker compose up --build` construye y levanta los tres servicios.
- La aplicación no opera antes de que Kafka y Oracle estén saludables.
- Oracle conserva datos al hacer `docker compose down` y volver a
  levantar.
- La app se ejecuta como usuario no root.
- No hay credenciales reales en el repositorio.

### Validación

```bash
docker compose config
docker compose up --build -d
docker compose ps
```

Flujo end-to-end: POST → esperar persistencia → GET y verificar count.

------------------------------------------------------------------------

## Fase 8 — JaCoCo, suite de tests y build final

### Alcance

- Configurar source set y tarea `integrationTest` (`src/intTest`).
- Configurar JaCoCo y verificación de umbrales.
- Integrar `check` con unit tests y cobertura.
- Habilitar integración condicional con `-PintegrationTest=true`.

### Criterios de aceptación

- `./gradlew build` ejecuta unit tests y genera el artefacto.
- `./gradlew build -PintegrationTest=true` ejecuta unit e integración.
- `check` verifica cobertura mínima del 80%.
- Los tests de integración no se ejecutan por defecto.

### Validación

```bash
./gradlew clean build
./gradlew clean build -PintegrationTest=true
```

------------------------------------------------------------------------

## Fase 9 — Documentación y entrega

### Alcance

- README con propósito, arquitectura, requisitos, comandos Docker y
  Gradle.
- Documentar `.env`, puertos, Swagger y ejemplos de requests.
- Documentar consistencia eventual, DLT, política de reintentos y
  limitaciones del entorno single-broker.

### Criterios de aceptación

- Una persona puede clonar el repo y seguir el README sin
  instrucciones verbales.
- El comando principal de ejecución es `docker compose up --build`.
- Los dos comandos de build están documentados.
- Swagger y endpoints están accesibles y documentados.
- No hay secretos ni archivos locales innecesarios en Git.

### Validación

- Seguir el README desde un checkout limpio.
- Ejecutar ambos comandos Gradle.
- Levantar stack desde cero y probar flujo end-to-end.
