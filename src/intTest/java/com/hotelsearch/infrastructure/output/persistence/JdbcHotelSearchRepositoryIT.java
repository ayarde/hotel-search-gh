package com.hotelsearch.infrastructure.output.persistence;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hotelsearch.OracleTestContainer;
import com.hotelsearch.application.port.out.HotelSearchRepository;
import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.HotelSearchCount;
import com.hotelsearch.domain.HotelSearchEvent;
import com.hotelsearch.domain.SearchId;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Repositorio de búsquedas contra Oracle AI Database Free real, con el esquema
 * creado por Flyway al arrancar la aplicación.
 */
@Testcontainers
@SpringBootTest
class JdbcHotelSearchRepositoryIT {

    @Container
    static final GenericContainer<?> ORACLE = OracleTestContainer.create();

    private static final String HOTEL_ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final String OTHER_HOTEL_ID = "6f1c2d3e-4b5a-4c6d-9e8f-0a1b2c3d4e5f";
    private static final LocalDate CHECK_IN = LocalDate.of(2026, 11, 9);
    private static final LocalDate CHECK_OUT = LocalDate.of(2026, 11, 29);
    private static final List<Integer> AGES = List.of(29, 28, 4, 3);
    private static final Instant CREATED_AT = Instant.parse("2026-11-17T12:00:00.123456Z");

    @Autowired
    private HotelSearchRepository repository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @DynamicPropertySource
    static void oracleProperties(DynamicPropertyRegistry registry) {
        OracleTestContainer.registerDataSource(registry, ORACLE);
    }

    @BeforeEach
    void deleteSearches() {
        jdbcClient.sql("delete from hotel_search").update();
    }

    @AfterEach
    void dropTestIndexes() {
        jdbcClient.sql("drop index if exists ux_test_created_at").update();
    }

    /**
     * Verifica cómo queda guardada una búsqueda nueva.
     *
     * Condiciones:
     * - searchId inexistente
     * - created_at con microsegundos
     *
     * Se espera:
     * - save devuelve true
     * - ages como JSON compacto en el orden recibido
     * - Fechas a medianoche
     * - created_at en UTC con precisión de microsegundos
     */
    @Test
    void save_NewEvent_StoresRowInAgreedFormat() {
        // given
        SearchId searchId = SearchId.generate();
        HotelSearchEvent event = event(searchId, search());

        // when
        boolean inserted = repository.save(event);

        // then
        Map<String, Object> row = storedRow(searchId);
        assertAll(
                () -> assertTrue(inserted, "Una búsqueda nueva debería insertarse"),
                () -> assertEquals(HOTEL_ID, row.get("hotel_id"), "hotel_id inesperado"),
                () -> assertEquals("[29,28,4,3]", row.get("ages"), "ages debería ser JSON compacto y ordenado"),
                () -> assertEquals("2026-11-09 00:00:00", row.get("check_in"), "check_in debería estar a medianoche"),
                () -> assertEquals("2026-11-29 00:00:00", row.get("check_out"), "check_out debería estar a medianoche"),
                () -> assertEquals("2026-11-17 12:00:00.123456", row.get("created_at"),
                        "created_at debería guardarse en UTC con microsegundos"));
    }

    /**
     * Verifica la lectura de una búsqueda sin otras equivalentes.
     *
     * Condiciones:
     * - Una sola búsqueda guardada
     *
     * Se espera:
     * - La búsqueda completa, igual a la guardada
     * - Count cero, sin contarse a sí misma
     */
    @Test
    void findWithMatchCount_OnlyStoredSearch_ReturnsZero() {
        // given
        SearchId searchId = SearchId.generate();
        repository.save(event(searchId, search()));

        // when
        Optional<HotelSearchCount> result = repository.findWithMatchCount(searchId);

        // then
        assertEquals(Optional.of(new HotelSearchCount(searchId, search(), 0)), result,
                "Debería devolverse la búsqueda guardada con count cero");
    }

    /**
     * Verifica que un searchId inexistente se distingue de un count cero.
     *
     * Condiciones:
     * - Otras búsquedas guardadas, ninguna con el searchId consultado
     *
     * Se espera:
     * - Optional vacío
     */
    @Test
    void findWithMatchCount_UnknownSearchId_ReturnsEmpty() {
        // given
        repository.save(event(SearchId.generate(), search()));

        // when
        Optional<HotelSearchCount> result = repository.findWithMatchCount(SearchId.generate());

        // then
        assertTrue(result.isEmpty(), "Un searchId inexistente debería devolver vacío");
    }

    /**
     * Verifica que solo cuentan las búsquedas que coinciden en todos los
     * campos, con las edades en el mismo orden.
     *
     * Condiciones:
     * - La búsqueda consultada y dos idénticas con otro searchId
     * - Búsquedas que difieren en hotel, check-in, check-out, orden de las
     *   edades o cantidad de edades
     *
     * Se espera:
     * - Count 2 para la consultada
     * - Count 0 para la que tiene las mismas edades en otro orden
     */
    @Test
    void findWithMatchCount_EquivalentAndDifferentSearches_CountsOnlyEquivalentOthers() {
        // given
        SearchId searchId = SearchId.generate();
        SearchId reorderedAgesId = SearchId.generate();
        repository.save(event(searchId, search()));
        repository.save(event(SearchId.generate(), search()));
        repository.save(event(SearchId.generate(), search()));
        repository.save(event(SearchId.generate(), HotelSearch.of(OTHER_HOTEL_ID, CHECK_IN, CHECK_OUT, AGES)));
        repository.save(event(SearchId.generate(), HotelSearch.of(HOTEL_ID, CHECK_IN.plusDays(1), CHECK_OUT, AGES)));
        repository.save(event(SearchId.generate(), HotelSearch.of(HOTEL_ID, CHECK_IN, CHECK_OUT.plusDays(1), AGES)));
        repository.save(event(SearchId.generate(), HotelSearch.of(HOTEL_ID, CHECK_IN, CHECK_OUT, List.of(29, 28, 4))));
        repository.save(event(reorderedAgesId, HotelSearch.of(HOTEL_ID, CHECK_IN, CHECK_OUT, List.of(3, 4, 28, 29))));

        // when
        long count = repository.findWithMatchCount(searchId).orElseThrow().count();
        long reorderedCount = repository.findWithMatchCount(reorderedAgesId).orElseThrow().count();

        // then
        assertAll(
                () -> assertEquals(2, count, "Solo deberían contarse las dos búsquedas idénticas"),
                () -> assertEquals(0, reorderedCount, "Las mismas edades en otro orden son otra búsqueda"));
    }

    /**
     * Verifica que una redelivery se descarta por la PK sin pisar la fila
     * original.
     *
     * Condiciones:
     * - Segundo save con el mismo searchId y otro contenido
     *
     * Se espera:
     * - Primer save true, segundo false, sin excepción
     * - Una sola fila, con el contenido original
     */
    @Test
    void save_SameSearchIdTwice_ReturnsFalseAndKeepsOriginalRow() {
        // given
        SearchId searchId = SearchId.generate();
        boolean firstInserted = repository.save(event(searchId, search()));
        HotelSearchEvent redelivery = event(searchId, HotelSearch.of(OTHER_HOTEL_ID, CHECK_IN, CHECK_OUT, AGES));

        // when
        boolean secondInserted = repository.save(redelivery);

        // then
        assertAll(
                () -> assertTrue(firstInserted, "El primer save debería insertar"),
                () -> assertFalse(secondInserted, "La redelivery debería informarse como duplicado"),
                () -> assertEquals(1, rowCount(), "Debería existir una sola fila"),
                () -> assertEquals(HOTEL_ID, storedRow(searchId).get("hotel_id"),
                        "La fila original no debería modificarse"));
    }

    /**
     * Verifica que redeliveries concurrentes del mismo evento insertan una
     * sola fila y ninguna falla.
     *
     * Condiciones:
     * - Ocho saves simultáneos del mismo evento en virtual threads
     *
     * Se espera:
     * - Exactamente un save devuelve true y el resto false
     * - Una sola fila
     */
    @Test
    void save_ConcurrentRedeliveries_InsertsOnce() throws Exception {
        // given
        int attempts = 8;
        HotelSearchEvent event = event(SearchId.generate(), search());
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();

        // when
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < attempts; i++) {
                results.add(executor.submit(() -> {
                    start.await();
                    return repository.save(event);
                }));
            }
            start.countDown();
        }

        // then
        long inserted = 0;
        for (Future<Boolean> result : results) {
            if (result.get()) {
                inserted++;
            }
        }
        long insertedCount = inserted;
        assertAll(
                () -> assertEquals(1, insertedCount, "Solo un save debería insertar"),
                () -> assertEquals(1, rowCount(), "Debería existir una sola fila"));
    }

    /**
     * Verifica que solo la PK prevista se trata como duplicado: una violación
     * de otra constraint unique se propaga.
     *
     * Condiciones:
     * - Índice unique adicional sobre created_at, creado solo para este test
     * - Dos eventos con distinto searchId y el mismo created_at
     *
     * Se espera:
     * - DataIntegrityViolationException en el segundo save
     * - Una sola fila
     */
    @Test
    void save_OtherUniqueConstraintViolated_PropagatesException() {
        // given
        jdbcClient.sql("create unique index ux_test_created_at on hotel_search (created_at)").update();
        repository.save(event(SearchId.generate(), search()));
        HotelSearchEvent sameCreatedAt = event(SearchId.generate(), search());

        // when
        Executable action = () -> repository.save(sameCreatedAt);

        // then
        assertThrows(DataIntegrityViolationException.class, action,
                "Una violación unique distinta de la PK debería propagarse");
        assertEquals(1, rowCount(), "El segundo evento no debería guardarse");
    }

    /**
     * Verifica que el INSERT participa de la transacción de Spring y se
     * revierte con ella.
     *
     * Condiciones:
     * - save exitoso dentro de una transacción que luego falla
     *
     * Se espera:
     * - La excepción de la transacción propagada
     * - La búsqueda no existe después del rollback
     */
    @Test
    void save_TransactionRolledBack_LeavesNoRow() {
        // given
        SearchId searchId = SearchId.generate();
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);

        // when
        Executable action = () -> transaction.executeWithoutResult(status -> {
            repository.save(event(searchId, search()));
            throw new IllegalStateException("fallo posterior al INSERT");
        });

        // then
        assertThrows(IllegalStateException.class, action, "El fallo de la transacción debería propagarse");
        assertTrue(repository.findWithMatchCount(searchId).isEmpty(), "El INSERT debería revertirse");
    }

    /**
     * Verifica que descartar un duplicado no invalida la transacción en
     * curso: Oracle revierte solo la sentencia fallida.
     *
     * Condiciones:
     * - En una misma transacción: save de A, redelivery de A y save de B
     *
     * Se espera:
     * - Resultados true, false y true
     * - Tras el commit, A y B guardadas
     */
    @Test
    void save_DuplicateInsideTransaction_CommitsOtherInserts() {
        // given
        SearchId firstId = SearchId.generate();
        SearchId secondId = SearchId.generate();
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);

        // when
        List<Boolean> results = transaction.execute(status -> List.of(
                repository.save(event(firstId, search())),
                repository.save(event(firstId, search())),
                repository.save(event(secondId, search()))));

        // then
        assertAll(
                () -> assertEquals(List.of(true, false, true), results, "Resultados inesperados en la transacción"),
                () -> assertTrue(repository.findWithMatchCount(firstId).isPresent(), "A debería quedar guardada"),
                () -> assertTrue(repository.findWithMatchCount(secondId).isPresent(), "B debería quedar guardada"));
    }

    // <editor-fold desc="Helpers">
    private static HotelSearch search() {
        return HotelSearch.of(HOTEL_ID, CHECK_IN, CHECK_OUT, AGES);
    }

    private static HotelSearchEvent event(SearchId searchId, HotelSearch search) {
        return new HotelSearchEvent(searchId, search, CREATED_AT);
    }

    private Map<String, Object> storedRow(SearchId searchId) {
        return jdbcClient.sql("""
                        select hotel_id,
                               ages,
                               to_char(check_in, 'yyyy-mm-dd hh24:mi:ss') as check_in,
                               to_char(check_out, 'yyyy-mm-dd hh24:mi:ss') as check_out,
                               to_char(created_at, 'yyyy-mm-dd hh24:mi:ss.ff6') as created_at
                          from hotel_search
                         where search_id = :searchId
                        """)
                .param("searchId", searchId.value().toString())
                .query()
                .singleRow();
    }

    private int rowCount() {
        return jdbcClient.sql("select count(*) from hotel_search").query(Integer.class).single();
    }
    // </editor-fold>
}
