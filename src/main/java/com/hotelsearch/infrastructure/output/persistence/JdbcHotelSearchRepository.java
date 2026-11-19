package com.hotelsearch.infrastructure.output.persistence;

import com.hotelsearch.application.port.out.HotelSearchRepository;
import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.HotelSearchCount;
import com.hotelsearch.domain.HotelSearchEvent;
import com.hotelsearch.domain.SearchId;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Repositorio de búsquedas sobre Oracle con Spring JDBC.
 *
 * <p>El INSERT va directo, sin consultar antes si el searchId existe: la PK es
 * la defensa contra las redeliveries de Kafka. Solo esa violación se trata como
 * duplicado; cualquier otro error SQL se propaga.
 *
 * <p>{@code created_at} se guarda en UTC porque la columna es un
 * {@code TIMESTAMP} sin zona horaria.
 *
 * <p>El bean se declara en {@code PersistenceConfiguration}.
 */
public class JdbcHotelSearchRepository implements HotelSearchRepository {

    // ORA-00001: violación de una constraint unique o primary key.
    private static final int UNIQUE_CONSTRAINT_VIOLATED = 1;

    // Oracle informa la constraint como "(ESQUEMA.NOMBRE)"; se compara el nombre
    // completo para no confundirla con otra que lo contenga como prefijo.
    private static final String PRIMARY_KEY_REFERENCE = ".pk_hotel_search)";

    private static final String INSERT_SQL = """
            insert into hotel_search (search_id, hotel_id, check_in, check_out, ages, created_at)
            values (:searchId, :hotelId, :checkIn, :checkOut, :ages, :createdAt)
            """;

    // Búsqueda y conteo en una sola sentencia. El índice idx_hotel_search_match
    // acota por hotel y fechas; la igualdad de ages se resuelve en el predicado.
    private static final String FIND_WITH_MATCH_COUNT_SQL = """
            select s.search_id,
                   s.hotel_id,
                   s.check_in,
                   s.check_out,
                   s.ages,
                   (select count(*)
                      from hotel_search other
                     where other.hotel_id = s.hotel_id
                       and other.check_in = s.check_in
                       and other.check_out = s.check_out
                       and other.ages = s.ages
                       and other.search_id <> s.search_id) as search_count
              from hotel_search s
             where s.search_id = :searchId
            """;

    private final JdbcClient jdbcClient;

    public JdbcHotelSearchRepository(JdbcClient jdbcClient) {
        this.jdbcClient = Objects.requireNonNull(jdbcClient, "jdbcClient");
    }

    @Override
    public boolean save(HotelSearchEvent event) {
        HotelSearch search = event.search();
        try {
            jdbcClient.sql(INSERT_SQL)
                    .param("searchId", event.searchId().value().toString())
                    .param("hotelId", search.hotelId().value().toString())
                    .param("checkIn", search.stayDates().checkIn())
                    .param("checkOut", search.stayDates().checkOut())
                    .param("ages", AgesColumn.toJson(search.ages()))
                    .param("createdAt", LocalDateTime.ofInstant(event.createdAt(), ZoneOffset.UTC))
                    .update();
            return true;
        } catch (DataIntegrityViolationException e) {
            if (isPrimaryKeyViolation(e)) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public Optional<HotelSearchCount> findWithMatchCount(SearchId searchId) {
        return jdbcClient.sql(FIND_WITH_MATCH_COUNT_SQL)
                .param("searchId", searchId.value().toString())
                .query(JdbcHotelSearchRepository::toHotelSearchCount)
                .optional();
    }

    private static HotelSearchCount toHotelSearchCount(ResultSet row, int rowNum) throws SQLException {
        HotelSearch search = HotelSearch.of(
                row.getString("hotel_id"),
                row.getObject("check_in", LocalDate.class),
                row.getObject("check_out", LocalDate.class),
                AgesColumn.fromJson(row.getString("ages")));
        return new HotelSearchCount(SearchId.of(row.getString("search_id")), search, row.getLong("search_count"));
    }

    // Se recorre la cadena de causas porque el driver puede anidar su propia
    // excepción debajo de la SQLException que envuelve Spring.
    private static boolean isPrimaryKeyViolation(DataIntegrityViolationException exception) {
        for (Throwable cause = exception.getCause(); cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sqlException) {
                String message = sqlException.getMessage();
                return sqlException.getErrorCode() == UNIQUE_CONSTRAINT_VIOLATED
                        && message != null
                        && message.toLowerCase(Locale.ROOT).contains(PRIMARY_KEY_REFERENCE);
            }
        }
        return false;
    }
}
