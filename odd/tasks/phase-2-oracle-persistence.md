# Phase 2 — Oracle persistence and migrations

Implements phase 2 of `IMPLEMENTATION-PLAN.md`: Spring JDBC + HikariCP, Flyway
migration for `hotel_search`, and the JDBC `HotelSearchRepository` adapter,
verified against a real Oracle Testcontainer.

## Specs

- S1 (plan, Alcance): "Configurar Spring JDBC y HikariCP."
- S2 (plan, Alcance): "Agregar Flyway y migración inicial de `hotel_search`."
- S3 (plan, Alcance): "Implementar `HotelSearchRepository`."
- S4 (plan, Alcance): "Consulta de búsqueda y count con una sola sentencia SQL."
- S5 (plan, Alcance): "Inserción y manejo de duplicado por PK sin SELECT previo."
- S6 (plan, Alcance): "Índice en `(hotel_id, check_in, check_out)`; igualdad de `ages` en predicado."
- S7 (criterio): "DDL cumple nombres SQL en minúsculas y modelo acordado." (model: `TECHNICAL-DECISIONS.md` §7)
- S8 (criterio): "`ages` se guarda como JSON compacto, orden preservado."
- S9 (criterio): "Consulta devuelve cero si no hay otras búsquedas iguales y excluye el `searchId` consultado."
- S10 (criterio): "`searchId` inexistente se distingue de count cero."
- S11 (criterio): "SQL usa bind parameters."
- S12 (criterio): "Solo la violación de PK prevista se trata como duplicado."
- S13 (validación): "Integration tests con Oracle Testcontainer real (`src/intTest`)."
- S14 (validación): "Verificar INSERT, lectura, igualdad de edades, constraints, rollback y duplicado."
- S15 (validación): "`./gradlew build -PintegrationTest=true`." and `./gradlew build` keeps passing.

## Tasks

- T1 [S1, S2, S6, S7] Dependencies (JDBC, Flyway, ojdbc, Testcontainers), datasource/Hikari config, `V1__create_hotel_search.sql`. Route: inline. Commit: 4f9c3ef (with `OracleSchemaIT`). Done.
- T2 [S3-S5, S8-S12] `JdbcHotelSearchRepository` + `AgesColumn` + unit tests. Route: inline. Commits: a3efa6e (`AgesColumn`), 0cf90bf (repository). Done.
- T3 [S13-S15] Oracle IT (`src/intTest`) with official image; relocate the app-start check out of unit tests. Route: inline. Commits: 4f9c3ef (schema IT), 0cf90bf (repository IT). Done.
- T4 Independent verification (Risk item 1: schema migration) and final builds. Route: gentle-ai-verify. Commit: n/a. Done: S1-S15 pass.

## Log

- L1 (user): "Implementa la fase 2 de IMPLEMENTATION-PLAN.md"
- L2 (user decision): Oracle IT image → "Oficial database/free:23.26.3.0-lite (Recomendada)": GenericContainer + explicit wait + `@DynamicPropertySource`; app user created by a startup script. The lite image runs `/opt/oracle/scripts/startup` after "DATABASE IS READY TO USE!" and then prints "The following output is now a tail of the alert.log:", which is the wait marker.
- L3 (decision): adding JDBC + Flyway makes the unit context test `HotelSearchApplicationTest` need a real database; the startup check moves to the Oracle IT (full `@SpringBootTest` + Flyway), the unit test is removed.
- L4 (evidence): the lite image has no `USERS` tablespace in FREEPDB1 (`ORA-00959`), so the IT app user gets `unlimited tablespace` instead of a quota on `users`. Phase 7 (Compose) needs the same app-user setup.
- L5 (evidence): `./gradlew clean build -PintegrationTest=true` green: `OracleSchemaIT` 3/3, `JdbcHotelSearchRepositoryIT` 9/9; `./gradlew clean build` green, JaCoCo lines 128/128, branches 39/40.
- L6 (evidence): independent gentle-ai-verify on 0cf90bf: S1-S15 pass, both builds green (unit 89/89; IT 9/9 + 3/3). Advisories: the count query omits `s.created_at` from the TECHNICAL-DECISIONS §7 example because `HotelSearchCount` does not carry it; `~/.testcontainers.properties` points at a missing Docker Desktop socket; two IT classes start two Oracle containers. RDD assess: reviewDue false (under budget).
- L7 (slice): 1,327 authored lines in 3 commits, past the ~400-line slice because the tests make up most of it. Commit boundaries: schema 4f9c3ef, ages mapping a3efa6e, repository 0cf90bf. The user chooses the delivery shape.
- L8 (user): "Haz commit solo cuando te lo pida. Usaste @Repository para crear un bean. Para creación beans deben hacer por Java Configurations"
- L9 (change): `@Repository` removed from `JdbcHotelSearchRepository`; the bean is declared in `infrastructure.config.PersistenceConfiguration`. Not committed: commits only on explicit request from now on.
