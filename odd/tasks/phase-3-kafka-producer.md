# Phase 3 — Kafka producer

Implements phase 3 of `IMPLEMENTATION-PLAN.md`: Spring Kafka producer and the
`HotelSearchPublisher` adapter, verified against a real Kafka Testcontainer.
Branch `feat/phase-3-kafka-producer`. Commits only on explicit user request (phase 2, L8).

## Specs

- S1 (plan, Alcance): "Configurar Spring Kafka producer."
- S2 (plan, Alcance): "Topic `hotel_availability_searches`."
- S3 (plan, Alcance): "Serialización JSON del evento."
- S4 (plan, Alcance): "Usar `searchId` como message key."
- S5 (plan, Alcance): "Configurar ACK, idempotencia del producer y timeouts." (`TECHNICAL-DECISIONS.md` §4: "Configurar `acks=all`.", "Configurar `enable.idempotence=true`.", "Configurar `delivery.timeout.ms=10000` y `request.timeout.ms=3000` como valores iniciales revisables mediante pruebas.", "Reintentos nativos del producer; no agregar RetryTemplate ni `@Retryable`.")
- S6 (plan, Alcance): "Implementar adaptador `HotelSearchPublisher`."
- S7 (criterio): "Evento publicado con `searchId` como key."
- S8 (criterio): "POST/caso de uso no informa éxito antes del ACK."
- S9 (criterio): "Fallo o timeout de publicación se traduce a error de disponibilidad."
- S10 (criterio): "No se agrega idempotencia HTTP ni Outbox."
- S11 (criterio): "No se abre transacción Oracle durante la publicación."
- S12 (validación): "Integration test con Kafka Testcontainer."
- S13 (validación): "Verificar key, payload y ACK."
- S14 (validación): "`./gradlew build -PintegrationTest=true`." and `./gradlew build` keeps passing.

## Tasks

- T1 [S1-S6, S10] Dependencies (`spring-boot-starter-kafka`, `testcontainers-kafka`), producer config in `application.yml`, `HotelSearchMessage` payload, `KafkaHotelSearchPublisher`, bean in `infrastructure.config`. Route: inline. Commit: c881a6e. Done.
- T2 [S7-S9, S11] Unit tests for the adapter (ACK wait, failure/timeout/interrupt translation) and mapping; ArchUnit rule: Kafka output adapter does not use Spring transactions. Route: inline. Commit: c881a6e. Done.
- T3 [S12-S14] `KafkaHotelSearchPublisherIT` with `apache/kafka:4.4.0`; existing Oracle ITs keep passing; both builds. Route: inline. Commit: c881a6e. Done.
- T4 Independent verification (Risk item 5: new dependencies and producer runtime config). Route: gentle-ai-verify. Done: S1-S14 pass (S9 503 mapping deferred to phase 5 by L5).

## Log

- L1 (user): "Implementa la Fase 3 de IMPLEMENTATION-PLAN.md"
- L2 (evidence): `apache/kafka:4.4.0` manifest exists for amd64 and arm64. Offline cache: `spring-kafka` 4.1.2 (has Jackson 3 `JacksonJsonSerializer`), `kafka-clients` 4.2.2, `testcontainers-kafka` 2.0.5 (`org.testcontainers.kafka.KafkaContainer`).
- L3 (decision): Kafka payload is an adapter record (`HotelSearchMessage`, ISO dates, `createdAt` as ISO instant) because ArchUnit keeps Jackson out of `domain`; type headers disabled so the consumer is not coupled to a producer class name. Phase 4 decides where the consumer reads that contract (ArchUnit forbids `input` → `output` dependencies).
- L4 (decision): `max.block.ms` set to 5000. Without it the client default (60 s) blocks `send()` while there is no broker metadata, so the POST would wait a minute before failing. Not in `TECHNICAL-DECISIONS.md` §4; added as a revisable timeout, same as the other two.
- L5 (scope): `SubmitHotelSearch`/`Clock` beans and the 503 mapping belong to phase 5 (REST). Topic creation (`NewTopic`) belongs to phase 4 ("Crear ambos topics explícitamente"); the IT relies on broker auto-creation.
- L6 (evidence): tests were written right after the adapter, not before. RED proxy: removing the `.get()` ACK wait makes 3 of 5 `KafkaHotelSearchPublisherTest` cases fail (ACK pending, delivery timeout, interrupt); restored.
- L7 (evidence, root cause): the first `KafkaHotelSearchPublisherIT` run failed `publish_BrokerUnreachable_*`: `KafkaTemplate.doSend` (spring-kafka 4.1.2, line 876) rethrows immediate failures as `org.springframework.kafka.KafkaException` ("Send failed", cause `TimeoutException: Topic ... not present in metadata after 5000 ms`), not as the client's `org.apache.kafka.common.KafkaException`. The adapter now translates both; unit test `publish_TemplateFailsImmediately_*` pins it.
- L8 (evidence): `./gradlew clean build -PintegrationTest=true --offline` green: 98 unit + 15 IT (`KafkaHotelSearchPublisherIT` 3/3, `JdbcHotelSearchRepositoryIT` 9/9, `OracleSchemaIT` 3/3), JaCoCo lines 159/159, branches 39/40. `./gradlew clean build --offline` green: lines 155/159, branches 39/40. Independent gentle-ai-verify reproduced both builds: S1-S14 pass. Advisories: worst-case POST latency is about 15 s (`max.block.ms` 5 s + `delivery.timeout.ms` 10 s), relevant for phase 5; the IT relies on topic auto-creation until phase 4. The IT config test was renamed to `getConfigurationProperties_*` per TESTING_RULES naming and re-run (3/3).
- L9 (RDD): assess escalated item 5 → independent verifier ran. Native review not started: the candidate is uncommitted and commits wait for the user's request.
- L10 (user): "Prepara el commit de la Fase 3"
- L11 (slice): phase 3 is c881a6e (756 lines: about 165 production, the rest tests and config) plus this evidence commit. Past the ~400-line slice because tests make up most of it and splitting them from the behavior would break the work unit. The branch holds phases 2 and 3 (7 commits since `main`); the user chooses the delivery shape.
- L12 (review): native review `review-4cd90d6c144463dc` over 7d552be..8f3fd19 (risk, resilience, reliability) approved with 5 advisory findings. R4-ambiguous-failure-may-still-deliver and R4-blocking-ack-wait-thread-saturation stay open for phase 5 (document 503 as "not confirmed", not "not written"; review the total wait bound with Virtual Threads).
- L13 (user): "aplicalos y deja un commit separado" (R3-untranslated-runtime-failures, R3-interrupt-flag-leak, R3-free-port-race).
- L14 (evidence): RED first: `publish_SendCancelled_*` and `publish_ProducerClosed_*` failed (2 of 8). The adapter now also translates `IllegalStateException` (closed producer; `CancellationException` extends it). The interrupt test clears the flag in `@AfterEach`. The IT keeps a loopback `ServerSocket` bound for the whole test (`publish_BrokerNotResponding_*`, 6.4 s) instead of closing a free port. `./gradlew clean build -PintegrationTest=true --offline` green: 100 unit + 15 IT, JaCoCo lines 161/161, branches 39/40; `./gradlew clean build --offline` green.
