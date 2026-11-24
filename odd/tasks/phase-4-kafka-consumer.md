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
- S18 (user, L17, review R4-001): "lo implementemos así queda en la fase 4". Design (L18): Oracle-unavailable failures are retried without limit with exponential back-off (1 s, x2, capped at 30 s) and never go to the DLT; the offset is not committed while they last. Infrastructure-unavailable = any exception in the cause chain that is `CannotCreateTransactionException`, `DataAccessResourceFailureException` (includes `CannotGetJdbcConnectionException`), `TransientDataAccessResourceException` or `RecoverableDataAccessException`. Every other non-permanent error keeps S4/S13: three retries at 1 s, then DLT. Permanent errors keep S5/S14.
- S17 (validación): "Integration tests con Kafka y Oracle reales." "Verificar persistencia, redelivery, rollback, reintentos y DLT." "`./gradlew build -PintegrationTest=true`." and `./gradlew build` keeps passing.

## Tasks

- T1 [S1-S8, S10, S12, S15, S16] Consumer config (`application.yml`, `KafkaConsumerConfiguration`: topics, DLT producer, recoverer, error handler, listener and `PersistHotelSearchUseCase` beans), `@Transactional` on `PersistHotelSearch` (+ ArchUnit allowance), consumer-side message record and listener, with unit tests. Route: inline. Commit: 0dcb495. Done (unit level, L6).
- T2 [S9-S15, S17] `HotelSearchConsumerIT` (Kafka + Oracle), keep Oracle-only ITs independent of Kafka, both builds. Route: gentle-ai-worker (L8). Commit: 0dcb495. Done (L9-L10).
- T3 [S18, S4, S13, S17] Infrastructure-unavailable back-off in `KafkaConsumerConfiguration` with unit and integration tests; update `TECHNICAL-DECISIONS.md` §5, `REQUIREMENTS.md` §7 and `IMPLEMENTATION-PLAN.md` phase 4; both builds. Route: gentle-ai-worker (context backstop). Commit: pending. Done (L19).

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
- L13 (user): "solo mergea los branchs de Fase 2 y 3 a main" / "si pushea". `main` fast-forwarded to 3f70549 and pushed to origin; phase-4 branch already contained it.
- L14 (review): native review `review-19eadda0c79f5927` over 0dcb495..7397138 (risk, resilience, reliability) approved with 3 advisory findings: R3-offset-after-commit-unproved, R3-permanent-classification-coverage, R4-001 (Oracle outage > ~3 s dead-letters valid searches).
- L15 (user): "Implementa R3-permanent-classification-coverage y R3-offset-after-commit-unproved". R4-001 explained, not implemented: it changes the retry policy fixed in TECHNICAL-DECISIONS §5; proposed as a documented limitation for phase 9.
- L16 (evidence): `HotelSearchListenerTest.onMessage_IncompleteOrMalformedFields_ThrowsInvalidHotelSearch` pins malformed searchId and missing hotelId/checkIn/checkOut/createdAt as `InvalidHotelSearchException` (behavior already correct, no RED). `HotelSearchConsumerIT` now samples the committed offset after the real INSERT on each of the 4 attempts and requires it never passes the record. `./gradlew clean build -PintegrationTest=true --offline` green: 115 unit + 21 IT (HotelSearchConsumerIT 6, KafkaHotelSearchPublisherIT 3, JdbcHotelSearchRepositoryIT 9, OracleSchemaIT 3), JaCoCo lines 201/201, branches 46/48. `./gradlew build --offline` green: lines 190/201, branches 46/48.
- L17 (user): "lo implementemos así queda en la fase 4" (R4-001). Explicit authorization to change the retry policy of TECHNICAL-DECISIONS §5.
- L18 (decision): Spring translates a real outage into `CannotCreateTransactionException` (transaction begin, Hikari timeout), `CannotGetJdbcConnectionException`, `DataAccessResourceFailureException`/`TransientDataAccessResourceException` (connection SQLExceptions) or `RecoverableDataAccessException` (Oracle `SQLRecoverableException`, connection lost mid-transaction). Those get `ExponentialBackOff(1000, 2.0)` with `maxInterval` 30 s and no attempt/elapsed limit, via `DefaultErrorHandler.setBackOffFunction`; the 30 s cap stays well below `max.poll.interval.ms` (300 s) because the handler sleeps on the consumer thread. Trade-off: a misclassified data error would block the single partition; only connection/transaction-start types are listed. Lock and timeout errors stay in the finite path.
- L19 (evidence): `KafkaConsumerConfiguration.backOffFor` walks the cause chain; the four outage types get `ExponentialBackOff(1000, 2.0)`, `maxInterval` 30 s, unlimited; anything else returns null (default `FixedBackOff(1000, 3)`). RED: new `KafkaConsumerConfigurationTest` cases did not compile, then 3 of 8 failed against a null stub. `resetStateOnExceptionChange` defaults to true in 4.1.2 (pinned by `hotelSearchErrorHandler_OutageThenTransientError_RestartsFiniteRetries`). The finite-path tests now use `CannotAcquireLockException`. New IT `onMessage_OracleUnavailableLongerThanRetries_PersistsWhenOracleReturns`: 4 `CannotGetJdbcConnectionException` failures then success, 5 attempts (~16 s), one row, offset never past the record while failing, nothing on the DLT. TECHNICAL-DECISIONS §5, REQUIREMENTS §7 and IMPLEMENTATION-PLAN phase 4 updated. `./gradlew clean build -PintegrationTest=true --offline` green: 119 unit + 22 IT (HotelSearchConsumerIT 7, KafkaHotelSearchPublisherIT 3, JdbcHotelSearchRepositoryIT 9, OracleSchemaIT 3), JaCoCo lines 209/209, branches 58/60. `./gradlew build --offline` green: lines 198/209, branches 58/60.
