package com.hotelsearch.domain;

import static com.hotelsearch.HotelSearchFixtures.SEARCH_ID;
import static com.hotelsearch.HotelSearchFixtures.search;
import static com.hotelsearch.domain.InvalidHotelSearchAssertions.assertInvalid;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class HotelSearchCountTest {

    /**
     * Verifica que el resultado conserva identificador, búsqueda y conteo.
     *
     * Condiciones:
     * - Todos los campos presentes
     * - Dos búsquedas equivalentes
     *
     * Se espera:
     * - Valores sin cambios
     */
    @Test
    void constructor_ValidValues_KeepsValues() {
        // given
        HotelSearch search = search();

        // when
        HotelSearchCount result = new HotelSearchCount(SEARCH_ID, search, 2);

        // then
        assertAll(
                () -> assertEquals(SEARCH_ID, result.searchId(), "El searchId debería conservarse"),
                () -> assertEquals(search, result.search(), "La búsqueda debería conservarse"),
                () -> assertEquals(2, result.count(), "El conteo debería conservarse"));
    }

    /**
     * Verifica que el resultado exige el searchId consultado.
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
        Executable action = () -> new HotelSearchCount(null, search, 2);

        // then
        assertInvalid(action, "searchId es obligatorio");
    }

    /**
     * Verifica que el resultado exige la búsqueda recuperada.
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
        Executable action = () -> new HotelSearchCount(SEARCH_ID, search, 2);

        // then
        assertInvalid(action, "search es obligatorio");
    }
}
