package com.hotelsearch.infrastructure.output.persistence;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.hotelsearch.OracleTestContainer;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Arranque de la aplicación contra Oracle real y esquema que deja la migración
 * Flyway. Las constraints se prueban con SQL directo porque protegen la tabla
 * de escrituras que no pasan por el dominio.
 */
@Testcontainers
@SpringBootTest
class OracleSchemaIT {

    @Container
    static final GenericContainer<?> ORACLE = OracleTestContainer.create();

    private static final String HOTEL_ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final LocalDate CHECK_IN = LocalDate.of(2026, 11, 9);
    private static final LocalDate CHECK_OUT = LocalDate.of(2026, 11, 29);

    @Autowired
    private JdbcClient jdbcClient;

    @DynamicPropertySource
    static void oracleProperties(DynamicPropertyRegistry registry) {
        OracleTestContainer.registerDataSource(registry, ORACLE);
    }

    @BeforeEach
    void deleteSearches() {
        jdbcClient.sql("delete from hotel_search").update();
    }

    /**
     * Verifica que la aplicación arranca contra Oracle y que Flyway deja el
     * esquema acordado en TECHNICAL-DECISIONS.md.
     *
     * Condiciones:
     * - Contexto de Spring levantado contra una base vacía
     *
     * Se espera:
     * - Migración V1 aplicada con éxito
     * - Columnas, tipos y nulabilidad de hotel_search según el modelo
     * - Solo la PK y el check de fechas como constraints con nombre
     * - Índice no único sobre (hotel_id, check_in, check_out) y ninguna otra
     *   constraint unique
     */
    @Test
    void migrate_ApplicationStartup_CreatesAgreedSchema() {
        // given
        String columnsSql = """
                select column_name || ' ' || data_type
                       || case when char_used is not null then '(' || char_length || ' ' || char_used || ')' end
                       || ' ' || nullable
                  from user_tab_columns
                 where table_name = 'HOTEL_SEARCH'
                 order by column_id
                """;

        // when
        Integer appliedMigrations = jdbcClient
                .sql("select count(*) from \"flyway_schema_history\" where \"version\" = '1' and \"success\" = 1")
                .query(Integer.class).single();
        List<String> columns = jdbcClient.sql(columnsSql).query(String.class).list();
        List<String> constraints = jdbcClient.sql("""
                select constraint_name || ' ' || constraint_type
                  from user_constraints
                 where table_name = 'HOTEL_SEARCH' and constraint_name not like 'SYS%'
                 order by constraint_name
                """).query(String.class).list();
        List<String> indexes = jdbcClient.sql("""
                select index_name || ' ' || uniqueness
                  from user_indexes
                 where table_name = 'HOTEL_SEARCH'
                 order by index_name
                """).query(String.class).list();
        List<String> matchIndexColumns = jdbcClient.sql("""
                select column_name
                  from user_ind_columns
                 where index_name = 'IDX_HOTEL_SEARCH_MATCH'
                 order by column_position
                """).query(String.class).list();

        // then
        assertAll(
                () -> assertEquals(1, appliedMigrations, "La migración V1 debería estar aplicada"),
                () -> assertEquals(List.of(
                        "SEARCH_ID VARCHAR2(36 C) N",
                        "HOTEL_ID VARCHAR2(36 C) N",
                        "CHECK_IN DATE N",
                        "CHECK_OUT DATE N",
                        "AGES VARCHAR2(600 C) N",
                        "CREATED_AT TIMESTAMP(6) N"), columns, "Las columnas no coinciden con el modelo acordado"),
                () -> assertEquals(List.of("CK_HOTEL_SEARCH_DATES C", "PK_HOTEL_SEARCH P"), constraints,
                        "Solo deberían existir la PK y el check de fechas"),
                () -> assertEquals(List.of("IDX_HOTEL_SEARCH_MATCH NONUNIQUE", "PK_HOTEL_SEARCH UNIQUE"), indexes,
                        "La PK debería ser el único índice unique"),
                () -> assertEquals(List.of("HOTEL_ID", "CHECK_IN", "CHECK_OUT"), matchIndexColumns,
                        "El índice de coincidencias debería cubrir hotel y fechas, sin ages"));
    }

    /**
     * Verifica la constraint de fechas, que protege la tabla de escrituras
     * que no pasan por el dominio.
     *
     * Condiciones:
     * - INSERT directo con check_out igual a check_in
     *
     * Se espera:
     * - DataIntegrityViolationException y ninguna fila guardada
     */
    @Test
    void insert_CheckOutNotAfterCheckIn_ViolatesCheckConstraint() {
        // given
        String insertSql = """
                insert into hotel_search (search_id, hotel_id, check_in, check_out, ages, created_at)
                values (:searchId, :hotelId, :checkIn, :checkOut, '[29]', systimestamp)
                """;

        // when
        Executable action = () -> jdbcClient.sql(insertSql)
                .param("searchId", UUID.randomUUID().toString())
                .param("hotelId", HOTEL_ID)
                .param("checkIn", CHECK_IN)
                .param("checkOut", CHECK_IN)
                .update();

        // then
        assertThrows(DataIntegrityViolationException.class, action,
                "check_in igual a check_out debería violar ck_hotel_search_dates");
        assertEquals(0, rowCount(), "No debería guardarse ninguna fila");
    }

    /**
     * Verifica que las columnas obligatorias rechazan nulos.
     *
     * Condiciones:
     * - INSERT directo sin ages
     *
     * Se espera:
     * - DataIntegrityViolationException y ninguna fila guardada
     */
    @Test
    void insert_MissingAges_ViolatesNotNullConstraint() {
        // given
        String insertSql = """
                insert into hotel_search (search_id, hotel_id, check_in, check_out, created_at)
                values (:searchId, :hotelId, :checkIn, :checkOut, systimestamp)
                """;

        // when
        Executable action = () -> jdbcClient.sql(insertSql)
                .param("searchId", UUID.randomUUID().toString())
                .param("hotelId", HOTEL_ID)
                .param("checkIn", CHECK_IN)
                .param("checkOut", CHECK_OUT)
                .update();

        // then
        assertThrows(DataIntegrityViolationException.class, action, "ages nulo debería rechazarse");
        assertEquals(0, rowCount(), "No debería guardarse ninguna fila");
    }

    // <editor-fold desc="Helpers">
    private int rowCount() {
        return jdbcClient.sql("select count(*) from hotel_search").query(Integer.class).single();
    }
    // </editor-fold>
}
