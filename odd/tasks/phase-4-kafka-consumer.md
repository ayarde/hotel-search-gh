# Phase 4 — Kafka consumer, persistence, retries and DLT

Implements phase 4 of `IMPLEMENTATION-PLAN.md`: listener on `hotel_availability_searches`, transactional
persistence in Oracle, `DefaultErrorHandler` retries and `hotel_search_dlt`, verified against real Kafka and
Oracle Testcontainers. Branch `feat/phase-4-kafka-consumer`. Commits only on explicit user request.

## Specs

- S1 (plan, Alcance): "Configurar listener para `hotel_availability_searches`."
- S2 (plan, Alcance): "Configurar `AckMode.RECORD`."
- S3 (plan, Alcance): "Implementar persistencia transaccional." (`TECHNICAL-DECISIONS.md` §6: "`@Transactional` en el servicio de aplicación que persiste.")
- S4 (plan, Alcance): "Configurar `DefaultErrorHandler`, tres reintentos adicionales con 1 segundo de intervalo."
- S5 (plan, Alcance): "Errores permanentes al DLT directamente."
- S6 (plan, Alcance): "Configurar `DeadLetterPublishingRecoverer` y topic `hotel_search_dlt`."
- S7 (plan, Alcance): "Crear ambos topics explícitamente con una partición y replication factor 1."
- S8 (criterio): "Consumer valida estructura básica."
- S9 (criterio): "Offset se confirma solo después del commit Oracle."
- S10 (criterio): "Oracle falla: rollback y excepción propagada."
- S11 (criterio): "Redelivery del mismo mensaje no crea una segunda fila."
- S12 (criterio): "Duplicado PK se descarta sin ocultar otros errores SQL."
- S13 (criterio): "Tras tres reintentos adicionales, error transitorio va al DLT."
- S14 (criterio): "Error permanente va directamente al DLT."
- S15 (criterio): "No hay consumidor automático del DLT."
- S16 (TECHNICAL-DECISIONS §5): "Si falla la publicación al DLT, no confirmar el offset original."
- S17 (validación): "Integration tests con Kafka y Oracle reales." "Verificar persistencia, redelivery, rollback, reintentos y DLT." "`./gradlew build -PintegrationTest=true`." and `./gradlew build` keeps passing.

## Tasks

- T1 [S1-S8, S10, S12, S15, S16] Consumer config (`application.yml`, `KafkaConsumerConfiguration`: topics, DLT producer, recoverer, error handler, listener and `PersistHotelSearchUseCase` beans), `@Transactional` on `PersistHotelSearch` (+ ArchUnit allowance), consumer-side message record and listener, with unit tests. Route: inline. Commit: 0dcb495. Done (unit level, L6).
- T2 [S9-S15, S17] `HotelSearchConsumerIT` (Kafka + Oracle), keep Oracle-only ITs independent of Kafka, both builds. Route: gentle-ai-worker (L8). Commit: 0dcb495. Done (L9-L10).

## Log

- L1 (user): "Implementa la fase 4 de IMPLEMENTATION-PLAN.md"
- L2 (decision): `@Transactional` goes on `PersistHotelSearch`, as `TECHNICAL-DECISIONS.md` §6/§8 say; ArchUnit lets `application` depend on `org.springframework.transaction.annotation` only (planned in phase 1).
- L3 (decision): the consumer reads its own record in `input.kafka` (ArchUnit forbids `input` → `output`). Both sides share the JSON contract, not a Java class; the IT publishes with the real producer adapter and consumes with the listener, which pins compatibility.
- L4 (decision): values are consumed as `byte[]` and parsed in the listener with Jackson; the DLT producer uses `ByteArraySerializer`, so every dead letter keeps the exact original bytes whether the failure was parsing, validation or Oracle. Unknown JSON properties are tolerated (Jackson 3 default) so the producer can add fields.
- L5 (decision): permanent errors = unreadable message and `InvalidHotelSearchException`, plus Spring Kafka's default fatal list. Everything else (Oracle, other SQL errors) is transient: `FixedBackOff(1000, 3)`, then DLT. Dead letters go to `hotel_search_dlt` with no fixed partition.
- L6 (evidence): RED first: `HotelSearchListenerTest`, `KafkaConsumerConfigurationTest` and `PersistHotelSearchTest.persist_Declaration_IsTransactional` did not compile before the production classes existed. GREEN: `./gradlew build --offline` passes, 114 unit tests, JaCoCo gate green. Unit tests cover classification (permanent recovered on the first attempt, transient on the fourth), DLT routing with original bytes and no fixed partition, and DLT send failure leaving the record unrecovered (S16).
- L7 (decision): the DLT `KafkaTemplate` is built inside the recoverer bean from Boot's `ProducerFactory` with a `ByteArraySerializer` override; declaring it as a bean would switch off Boot's `KafkaTemplate` autoconfiguration that the producer adapter uses.
- L8 (route): context backstop reached before T2; T2 delegated to a worker.
- L9 (evidence): `HotelSearchConsumerIT` (6 tests: topics S7; producer adapter -> row + committed offset S1/S2/S9; same event twice -> one row S11/S12; Oracle failure after a real INSERT via `@MockitoSpyBean` repository -> 4 attempts >= 2.9 s apart, 0 rows, offset not committed during the first attempt, DLT byte-identical S9/S10/S13; invalid ages and malformed JSON -> DLT without retries, broker timestamps < 3 s apart S14; only the main topic has listeners S15). Oracle-only ITs start with `spring.kafka.listener.auto-startup=false` and `spring.kafka.admin.auto-create=false`. `./gradlew build -PintegrationTest=true --offline` green: 114 unit + 21 IT (OracleSchemaIT 3, JdbcHotelSearchRepositoryIT 9, KafkaHotelSearchPublisherIT 3, HotelSearchConsumerIT 6), JaCoCo lines 201/201, branches 46/48. `./gradlew build --offline` green: lines 190/201, branches 46/48.
- L10 (evidence): RED proxy: removing `@Transactional` from `PersistHotelSearch.persist` makes `onMessage_OracleFailsOnEveryAttempt_RollsBackRetriesAndSendsToDlt` fail (1 of 6); restored. The failure injection is a spy on the repository port that runs the real INSERT first; Oracle and Kafka stay real.
- L11 (user): "Haz los commits"
- L12 (slice): phase 4 is 0dcb495 (1253 insertions, 8 deletions; about 260 production lines, the rest tests and config) plus this evidence commit. Past the ~400-line slice because the tests belong to the same work unit. The branch holds phases 2-4 (10 commits since `main`); the user chooses the delivery shape.
