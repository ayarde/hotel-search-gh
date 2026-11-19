package com.hotelsearch.domain;

import static com.hotelsearch.HotelSearchFixtures.CREATED_AT;
import static com.hotelsearch.HotelSearchFixtures.SEARCH_ID;
import static com.hotelsearch.HotelSearchFixtures.search;
import static com.hotelsearch.domain.InvalidHotelSearchAssertions.assertInvalid;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class HotelSearchEventTest {

    /**
     * Verifica que el evento conserva identificador, búsqueda y momento de
     * creación.
     *
     * Condiciones:
     * - Todos los campos presentes
     *
     * Se espera:
     * - Valores sin cambios
     */
    @Test
    void constructor_ValidValues_KeepsValues() {
        // given
        HotelSearch search = search();

        // when
        HotelSearchEvent event = new HotelSearchEvent(SEARCH_ID, search, CREATED_AT);

        // then
        assertAll(
                () -> assertEquals(SEARCH_ID, event.searchId(), "El searchId debería conservarse"),
                () -> assertEquals(search, event.search(), "La búsqueda debería conservarse"),
                () -> assertEquals(CREATED_AT, event.createdAt(), "El createdAt debería conservarse"));
    }

    /**
     * Verifica que el evento exige searchId; es la key del mensaje y la PK.
     *
     * Condiciones:
     * - searchId null
     *
     * Se espera:
     * - InvalidHotelSearchException nombrando searchId
     */
    @Test
    void constructor_NullSearchId_ThrowsInvalidHotelSearch() {
        // given
        HotelSearch search = search();

        // when
        Executable action = () -> new HotelSearchEvent(null, search, CREATED_AT);

        // then
        assertInvalid(action, "searchId es obligatorio");
    }

    /**
     * Verifica que el evento exige la búsqueda.
     *
     * Condiciones:
     * - search null
     *
     * Se espera:
     * - InvalidHotelSearchException nombrando search
     */
    @Test
    void constructor_NullSearch_ThrowsInvalidHotelSearch() {
        // given
        HotelSearch search = null;

        // when
        Executable action = () -> new HotelSearchEvent(SEARCH_ID, search, CREATED_AT);

        // then
        assertInvalid(action, "search es obligatorio");
    }

    /**
     * Verifica que el evento exige el momento de creación.
     *
     * Condiciones:
     * - createdAt null
     *
     * Se espera:
     * - InvalidHotelSearchException nombrando createdAt
     */
    @Test
    void constructor_NullCreatedAt_ThrowsInvalidHotelSearch() {
        // given
        HotelSearch search = search();

        // when
        Executable action = () -> new HotelSearchEvent(SEARCH_ID, search, null);

        // then
        assertInvalid(action, "createdAt es obligatorio");
    }
}
