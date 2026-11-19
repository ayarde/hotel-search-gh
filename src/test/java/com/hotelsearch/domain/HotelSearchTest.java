package com.hotelsearch.domain;

import static com.hotelsearch.HotelSearchFixtures.AGES;
import static com.hotelsearch.HotelSearchFixtures.CHECK_IN;
import static com.hotelsearch.HotelSearchFixtures.CHECK_OUT;
import static com.hotelsearch.HotelSearchFixtures.HOTEL_ID;
import static com.hotelsearch.HotelSearchFixtures.search;
import static com.hotelsearch.domain.InvalidHotelSearchAssertions.assertInvalid;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class HotelSearchTest {

    /**
     * Verifica que la búsqueda se construye a partir de valores primitivos
     * válidos.
     *
     * Condiciones:
     * - hotelId canónico, fechas ordenadas y edades válidas
     *
     * Se espera:
     * - Value objects con los valores recibidos
     */
    @Test
    void of_ValidValues_BuildsSearch() {
        // given
        String hotelId = HOTEL_ID;

        // when
        HotelSearch search = HotelSearch.of(hotelId, CHECK_IN, CHECK_OUT, AGES);

        // then
        assertAll(
                () -> assertEquals(HotelId.of(HOTEL_ID), search.hotelId(), "El hotelId debería conservarse"),
                () -> assertEquals(new StayDates(CHECK_IN, CHECK_OUT), search.stayDates(), "Las fechas deberían conservarse"),
                () -> assertEquals(new Ages(AGES), search.ages(), "Las edades deberían conservarse"));
    }

    /**
     * Verifica que of propaga la validación de los value objects.
     *
     * Condiciones:
     * - hotelId con espacio final
     *
     * Se espera:
     * - InvalidHotelSearchException del hotelId
     */
    @Test
    void of_InvalidHotelId_ThrowsInvalidHotelSearch() {
        // given
        String hotelId = HOTEL_ID + " ";

        // when
        Executable action = () -> HotelSearch.of(hotelId, CHECK_IN, CHECK_OUT, AGES);

        // then
        assertInvalid(action, "hotelId debe ser un UUID canónico de 36 caracteres");
    }

    /**
     * Verifica que hotelId es obligatorio en el constructor canónico.
     *
     * Condiciones:
     * - HotelId null
     *
     * Se espera:
     * - InvalidHotelSearchException nombrando hotelId
     */
    @Test
    void constructor_NullHotelId_ThrowsInvalidHotelSearch() {
        // given
        HotelSearch valid = search();

        // when
        Executable action = () -> new HotelSearch(null, valid.stayDates(), valid.ages());

        // then
        assertInvalid(action, "hotelId es obligatorio");
    }

    /**
     * Verifica que las fechas son obligatorias en el constructor canónico.
     *
     * Condiciones:
     * - StayDates null
     *
     * Se espera:
     * - InvalidHotelSearchException nombrando stayDates
     */
    @Test
    void constructor_NullStayDates_ThrowsInvalidHotelSearch() {
        // given
        HotelSearch valid = search();

        // when
        Executable action = () -> new HotelSearch(valid.hotelId(), null, valid.ages());

        // then
        assertInvalid(action, "stayDates es obligatorio");
    }

    /**
     * Verifica que las edades son obligatorias en el constructor canónico.
     *
     * Condiciones:
     * - Ages null
     *
     * Se espera:
     * - InvalidHotelSearchException nombrando ages
     */
    @Test
    void constructor_NullAges_ThrowsInvalidHotelSearch() {
        // given
        HotelSearch valid = search();

        // when
        Executable action = () -> new HotelSearch(valid.hotelId(), valid.stayDates(), null);

        // then
        assertInvalid(action, "ages es obligatorio");
    }

    /**
     * Verifica que dos búsquedas con los mismos campos son iguales; esa
     * igualdad define qué búsquedas se cuentan como equivalentes.
     *
     * Condiciones:
     * - Mismos hotelId, fechas y edades en el mismo orden
     *
     * Se espera:
     * - Búsquedas iguales
     */
    @Test
    void equals_SameFields_AreEqual() {
        // given
        HotelSearch search = search();

        // when
        HotelSearch other = HotelSearch.of(HOTEL_ID, CHECK_IN, CHECK_OUT, AGES);

        // then
        assertEquals(search, other, "Búsquedas con los mismos campos deberían ser iguales");
    }
}
