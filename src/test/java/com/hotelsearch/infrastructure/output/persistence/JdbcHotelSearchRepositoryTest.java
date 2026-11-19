package com.hotelsearch.infrastructure.output.persistence;

import static com.hotelsearch.HotelSearchFixtures.CHECK_IN;
import static com.hotelsearch.HotelSearchFixtures.CHECK_OUT;
import static com.hotelsearch.HotelSearchFixtures.HOTEL_ID;
import static com.hotelsearch.HotelSearchFixtures.SEARCH_ID;
import static com.hotelsearch.HotelSearchFixtures.event;
import static com.hotelsearch.HotelSearchFixtures.search;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hotelsearch.domain.HotelSearchCount;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.core.simple.JdbcClient.MappedQuerySpec;
import org.springframework.jdbc.core.simple.JdbcClient.StatementSpec;

/**
 * Cubre la traducción entre el dominio y los parámetros/filas JDBC, y la
 * clasificación de errores de integridad. El comportamiento contra Oracle
 * real se prueba en {@code JdbcHotelSearchRepositoryIT}.
 */
@ExtendWith(MockitoExtension.class)
class JdbcHotelSearchRepositoryTest {

    private static final String PK_VIOLATION_MESSAGE =
            "ORA-00001: unique constraint (HOTEL_SEARCH.PK_HOTEL_SEARCH) violated on table HOTEL_SEARCH";

    @Mock
    private JdbcClient jdbcClient;

    @Mock
    private StatementSpec statement;

    private JdbcHotelSearchRepository repository;

    @BeforeEach
    void setUp() {
        when(jdbcClient.sql(anyString())).thenReturn(statement);
        when(statement.param(anyString(), any())).thenReturn(statement);
        repository = new JdbcHotelSearchRepository(jdbcClient);
    }

    /**
     * Verifica que cada columna se envía como bind parameter con el valor
     * convertido al formato de la tabla.
     *
     * Condiciones:
     * - Evento válido
     * - El INSERT afecta una fila
     *
     * Se espera:
     * - UUIDs como texto, fechas como LocalDate, ages como JSON compacto y
     *   created_at en UTC
     * - Resultado true
     */
    @Test
    void save_NewEvent_BindsColumnsAndReturnsTrue() {
        // given
        when(statement.update()).thenReturn(1);

        // when
        boolean inserted = repository.save(event());

        // then
        assertTrue(inserted, "Un INSERT exitoso debería informarse como insertado");
        verify(statement).param("searchId", SEARCH_ID.value().toString());
        verify(statement).param("hotelId", HOTEL_ID);
        verify(statement).param("checkIn", CHECK_IN);
        verify(statement).param("checkOut", CHECK_OUT);
        verify(statement).param("ages", "[29,28,4,3]");
        verify(statement).param("createdAt", LocalDateTime.of(2026, 11, 17, 12, 0));
    }

    /**
     * Verifica que la violación de la PK se trata como redelivery, aun cuando
     * el driver anida su propia excepción debajo de la SQLException.
     *
     * Condiciones:
     * - ORA-00001 sobre pk_hotel_search
     *
     * Se espera:
     * - Resultado false, sin excepción
     */
    @Test
    void save_PrimaryKeyViolation_ReturnsFalse() {
        // given
        SQLException driverError = new SQLIntegrityConstraintViolationException(PK_VIOLATION_MESSAGE, "23000", 1);
        driverError.initCause(new IllegalStateException("detalle interno del driver"));
        when(statement.update()).thenThrow(new DataIntegrityViolationException("duplicado", driverError));

        // when
        boolean inserted = repository.save(event());

        // then
        assertFalse(inserted, "La violación de la PK debería informarse como duplicado");
    }

    /**
     * Verifica que una violación unique sobre otra constraint no se confunde
     * con un duplicado, aunque su nombre empiece igual que el de la PK.
     *
     * Condiciones:
     * - ORA-00001 sobre pk_hotel_search_other
     *
     * Se espera:
     * - La excepción original propagada
     */
    @Test
    void save_OtherUniqueConstraintViolation_PropagatesException() {
        // given
        SQLException driverError = new SQLIntegrityConstraintViolationException(
                "ORA-00001: unique constraint (HOTEL_SEARCH.PK_HOTEL_SEARCH_OTHER) violated", "23000", 1);
        DataIntegrityViolationException failure = new DataIntegrityViolationException("unique", driverError);
        when(statement.update()).thenThrow(failure);

        // when
        Executable action = () -> repository.save(event());

        // then
        assertSame(failure, assertThrows(DataIntegrityViolationException.class, action,
                "Otra constraint unique no debería tratarse como duplicado"), "Debería propagarse la excepción original");
    }

    /**
     * Verifica que un error de integridad con otro código Oracle se propaga,
     * aunque su mensaje mencione la PK.
     *
     * Condiciones:
     * - ORA-02290 (check constraint) con el nombre de la PK en el mensaje
     *
     * Se espera:
     * - La excepción original propagada
     */
    @Test
    void save_OtherIntegrityErrorCode_PropagatesException() {
        // given
        SQLException driverError = new SQLIntegrityConstraintViolationException(
                "ORA-02290: check constraint (HOTEL_SEARCH.PK_HOTEL_SEARCH) violated", "23000", 2290);
        DataIntegrityViolationException failure = new DataIntegrityViolationException("check", driverError);
        when(statement.update()).thenThrow(failure);

        // when
        Executable action = () -> repository.save(event());

        // then
        assertSame(failure, assertThrows(DataIntegrityViolationException.class, action,
                "Un error distinto de ORA-00001 no debería tratarse como duplicado"),
                "Debería propagarse la excepción original");
    }

    /**
     * Verifica que un error de integridad sin SQLException asociada no se
     * descarta.
     *
     * Condiciones:
     * - DataIntegrityViolationException sin causa
     *
     * Se espera:
     * - La excepción original propagada
     */
    @Test
    void save_IntegrityViolationWithoutSqlCause_PropagatesException() {
        // given
        DataIntegrityViolationException failure = new DataIntegrityViolationException("sin causa");
        when(statement.update()).thenThrow(failure);

        // when
        Executable action = () -> repository.save(event());

        // then
        assertSame(failure, assertThrows(DataIntegrityViolationException.class, action,
                "Sin SQLException no hay forma de confirmar un duplicado"), "Debería propagarse la excepción original");
    }

    /**
     * Verifica que una SQLException sin mensaje no rompe la clasificación.
     *
     * Condiciones:
     * - ORA-00001 sin texto de mensaje
     *
     * Se espera:
     * - La excepción original propagada
     */
    @Test
    void save_UniqueViolationWithoutMessage_PropagatesException() {
        // given
        SQLException driverError = new SQLIntegrityConstraintViolationException(null, "23000", 1);
        DataIntegrityViolationException failure = new DataIntegrityViolationException("unique", driverError);
        when(statement.update()).thenThrow(failure);

        // when
        Executable action = () -> repository.save(event());

        // then
        assertSame(failure, assertThrows(DataIntegrityViolationException.class, action,
                "Sin el nombre de la constraint no hay forma de confirmar un duplicado"),
                "Debería propagarse la excepción original");
    }

    /**
     * Verifica que la fila de la consulta se traduce a la búsqueda y su
     * conteo, usando el searchId como bind parameter.
     *
     * Condiciones:
     * - La consulta devuelve una fila con dos búsquedas equivalentes
     *
     * Se espera:
     * - HotelSearchCount con searchId, búsqueda completa y count 2
     */
    @Test
    void findWithMatchCount_ExistingSearch_ReturnsSearchAndCount() throws SQLException {
        // given
        ResultSet row = resultSetForStoredSearch(2);
        answerQueryWith(row);

        // when
        Optional<HotelSearchCount> result = repository.findWithMatchCount(SEARCH_ID);

        // then
        assertEquals(Optional.of(new HotelSearchCount(SEARCH_ID, search(), 2)), result,
                "La fila debería traducirse a la búsqueda guardada y su conteo");
        verify(statement).param("searchId", SEARCH_ID.value().toString());
    }

    /**
     * Verifica que la ausencia de fila se informa como vacío y no como count
     * cero.
     *
     * Condiciones:
     * - La consulta no devuelve filas
     *
     * Se espera:
     * - Optional vacío
     */
    @Test
    void findWithMatchCount_UnknownSearch_ReturnsEmpty() {
        // given
        answerQueryWith(null);

        // when
        Optional<HotelSearchCount> result = repository.findWithMatchCount(SEARCH_ID);

        // then
        assertTrue(result.isEmpty(), "Un searchId inexistente debería devolver vacío");
    }

    // <editor-fold desc="Helpers">
    private ResultSet resultSetForStoredSearch(long count) throws SQLException {
        ResultSet row = mock(ResultSet.class);
        when(row.getString("search_id")).thenReturn(SEARCH_ID.value().toString());
        when(row.getString("hotel_id")).thenReturn(HOTEL_ID);
        when(row.getObject("check_in", LocalDate.class)).thenReturn(CHECK_IN);
        when(row.getObject("check_out", LocalDate.class)).thenReturn(CHECK_OUT);
        when(row.getString("ages")).thenReturn("[29,28,4,3]");
        when(row.getLong("search_count")).thenReturn(count);
        return row;
    }

    /**
     * Simula la consulta: con fila, aplica el RowMapper del repositorio sobre
     * ella; sin fila, devuelve vacío.
     */
    @SuppressWarnings("unchecked")
    private void answerQueryWith(ResultSet row) {
        when(statement.query(any(RowMapper.class))).thenAnswer(invocation -> {
            RowMapper<HotelSearchCount> mapper = invocation.getArgument(0);
            MappedQuerySpec<HotelSearchCount> mapped = mock(MappedQuerySpec.class);
            HotelSearchCount mappedRow = row == null ? null : mapper.mapRow(row, 0);
            when(mapped.optional()).thenReturn(Optional.ofNullable(mappedRow));
            return mapped;
        });
    }
    // </editor-fold>
}
