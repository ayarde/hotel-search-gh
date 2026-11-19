package com.hotelsearch;

import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.HotelSearchEvent;
import com.hotelsearch.domain.SearchId;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/**
 * Datos válidos de referencia para los tests. Cada test cambia solo el valor
 * que le interesa probar.
 */
public final class HotelSearchFixtures {

    public static final String HOTEL_ID = "550e8400-e29b-41d4-a716-446655440000";
    public static final LocalDate CHECK_IN = LocalDate.of(2026, 11, 9);
    public static final LocalDate CHECK_OUT = LocalDate.of(2026, 11, 29);
    public static final List<Integer> AGES = List.of(29, 28, 4, 3);
    public static final SearchId SEARCH_ID = SearchId.of("3f2c8a1e-9b4d-4c6e-8f1a-2b3c4d5e6f70");
    public static final Instant CREATED_AT = Instant.parse("2026-11-17T12:00:00Z");

    private HotelSearchFixtures() {
    }

    public static HotelSearch search() {
        return HotelSearch.of(HOTEL_ID, CHECK_IN, CHECK_OUT, AGES);
    }

    public static HotelSearch searchWithCheckIn(LocalDate checkIn) {
        return HotelSearch.of(HOTEL_ID, checkIn, checkIn.plusDays(5), AGES);
    }

    public static HotelSearchEvent event() {
        return new HotelSearchEvent(SEARCH_ID, search(), CREATED_AT);
    }
}
