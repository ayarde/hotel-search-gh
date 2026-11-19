package com.hotelsearch.infrastructure.input.kafka;

import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.HotelSearchEvent;
import com.hotelsearch.domain.SearchId;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Lectura del JSON publicado en {@code hotel_availability_searches}.
 *
 * <p>El contrato compartido con el producer es el JSON, no una clase Java:
 * fechas ISO-8601 y {@code createdAt} como instante ISO. Los campos pueden
 * llegar nulos; la validación queda a cargo del dominio al construir el
 * evento, con los mismos mensajes que en el resto de la aplicación.
 */
record ConsumedHotelSearchMessage(
        String searchId,
        String hotelId,
        LocalDate checkIn,
        LocalDate checkOut,
        List<Integer> ages,
        Instant createdAt) {

    ConsumedHotelSearchMessage {
        // List.copyOf rechaza elementos null; esos casos los informa Ages.
        ages = ages == null ? null : Collections.unmodifiableList(new ArrayList<>(ages));
    }

    HotelSearchEvent toEvent() {
        return new HotelSearchEvent(
                SearchId.of(searchId),
                HotelSearch.of(hotelId, checkIn, checkOut, ages),
                createdAt);
    }
}
