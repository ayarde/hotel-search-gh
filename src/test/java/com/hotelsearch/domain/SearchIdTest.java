package com.hotelsearch.domain;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class SearchIdTest {

    /**
     * Verifica que el servidor genera identificadores UUIDv4.
     *
     * Condiciones:
     * - Generación sin parámetros
     *
     * Se espera:
     * - UUID versión 4 con variante RFC 9562
     */
    @Test
    void generate_NewId_IsRandomVersion4Uuid() {
        // given
        int expectedVersion = 4;

        // when
        SearchId searchId = SearchId.generate();

        // then
        assertAll(
                () -> assertEquals(expectedVersion, searchId.value().version(), "El searchId debería ser UUIDv4"),
                () -> assertEquals(2, searchId.value().variant(), "El searchId debería usar la variante RFC 9562"));
    }

    /**
     * Verifica que cada generación produce un identificador nuevo.
     *
     * Condiciones:
     * - Dos generaciones consecutivas
     *
     * Se espera:
     * - Identificadores distintos
     */
    @Test
    void generate_ConsecutiveCalls_ReturnDistinctIds() {
        // given
        SearchId first = SearchId.generate();

        // when
        SearchId second = SearchId.generate();

        // then
        assertNotEquals(first, second, "Cada generación debería producir un searchId distinto");
    }

    /**
     * Verifica que un searchId recibido en texto canónico se interpreta
     * correctamente.
     *
     * Condiciones:
     * - Texto UUID canónico
     *
     * Se espera:
     * - SearchId con el UUID equivalente
     */
    @Test
    void of_CanonicalUuid_ReturnsSearchId() {
        // given
        String text = "3f2c8a1e-9b4d-4c6e-8f1a-2b3c4d5e6f70";

        // when
        SearchId searchId = SearchId.of(text);

        // then
        assertEquals(UUID.fromString(text), searchId.value(), "El searchId debería conservar el UUID recibido");
    }

    /**
     * Verifica que un searchId mal formado se rechaza nombrando el campo.
     *
     * Condiciones:
     * - Texto en forma abreviada, aceptado por UUID.fromString
     *
     * Se espera:
     * - InvalidHotelSearchException con el formato esperado
     */
    @Test
    void of_NonCanonicalText_ThrowsInvalidHotelSearch() {
        // given
        String text = "1-1-1-1-1";

        // when
        Executable action = () -> SearchId.of(text);

        // then
        InvalidHotelSearchException exception = assertThrows(InvalidHotelSearchException.class, action,
                "Un searchId no canónico debería rechazarse");
        assertEquals("searchId debe ser un UUID canónico de 36 caracteres", exception.getMessage(),
                "El mensaje debería nombrar el campo y el formato");
    }

    /**
     * Verifica que no se puede construir un SearchId sin valor.
     *
     * Condiciones:
     * - UUID null
     *
     * Se espera:
     * - InvalidHotelSearchException indicando que el campo es obligatorio
     */
    @Test
    void constructor_NullValue_ThrowsInvalidHotelSearch() {
        // given
        UUID value = null;

        // when
        Executable action = () -> new SearchId(value);

        // then
        InvalidHotelSearchException exception = assertThrows(InvalidHotelSearchException.class, action,
                "Un searchId null debería rechazarse");
        assertEquals("searchId es obligatorio", exception.getMessage(), "El mensaje debería nombrar el campo");
    }
}
