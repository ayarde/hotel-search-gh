package com.hotelsearch.infrastructure.output.kafka;

import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.HotelSearchEvent;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/**
 * Payload JSON que se publica en {@code hotel_availability_searches}.
 *
 * <p>Es el contrato del mensaje, separado del evento de dominio para que el
 * formato en Kafka no dependa de cómo están modelados los value objects. Las
 * fechas viajan en ISO-8601 ({@code 2026-11-09}); el formato
 * {@code dd/MM/yyyy} es solo del contrato HTTP.
 */
public record HotelSearchMessage(
        String searchId,
        String hotelId,
        LocalDate checkIn,
        LocalDate checkOut,
        List<Integer> ages,
        Instant createdAt) {

    public HotelSearchMessage {
        ages = List.copyOf(ages);
    }

    static HotelSearchMessage from(HotelSearchEvent event) {
        HotelSearch search = event.search();
        return new HotelSearchMessage(
                event.searchId().value().toString(),
                search.hotelId().value().toString(),
                search.stayDates().checkIn(),
                search.stayDates().checkOut(),
                search.ages().values(),
                event.createdAt());
    }
}
