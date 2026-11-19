package com.hotelsearch.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HotelIdTest {

    /**
     * Verifica que un UUID en forma canónica se acepta tal cual llega.
     *
     * Condiciones:
     * - Texto de 36 caracteres con formato 8-4-4-4-12
     *
     * Se espera:
     * - HotelId con el UUID equivalente al texto recibido
     */
    @Test
    void of_CanonicalUuid_ReturnsHotelId() {
        // given
        String text = "550e8400-e29b-41d4-a716-446655440000";

        // when
        HotelId hotelId = HotelId.of(text);

        // then
        assertEquals(UUID.fromString(text), hotelId.value(), "El hotelId debería conservar el UUID recibido");
    }

    /**
     * Verifica que mayúsculas y minúsculas representan el mismo hotel.
     *
     * Condiciones:
     * - Mismo UUID escrito en mayúsculas y en minúsculas
     *
     * Se espera:
     * - Ambos HotelId son iguales, de modo que cuentan como la misma búsqueda
     */
    @Test
    void of_UppercaseUuid_EqualsLowercaseHotelId() {
        // given
        String lowercase = "550e8400-e29b-41d4-a716-446655440000";
        String uppercase = lowercase.toUpperCase();

        // when
        HotelId fromUppercase = HotelId.of(uppercase);

        // then
        assertEquals(HotelId.of(lowercase), fromUppercase, "La capitalización no debería cambiar la identidad del hotel");
    }

    /**
     * Verifica que hotelId es obligatorio.
     *
     * Condiciones:
     * - Texto null
     *
     * Se espera:
     * - InvalidHotelSearchException indicando que el campo es obligatorio
     */
    @Test
    void of_NullText_ThrowsInvalidHotelSearch() {
        // given
        String text = null;

        // when
        Executable action = () -> HotelId.of(text);

        // then
        InvalidHotelSearchException exception = assertThrows(InvalidHotelSearchException.class, action,
                "Un hotelId null debería rechazarse");
        assertEquals("hotelId es obligatorio", exception.getMessage(), "El mensaje debería nombrar el campo");
    }

    /**
     * Verifica que se rechaza todo texto que no sea un UUID canónico, sin
     * normalizarlo. UUID.fromString acepta formas abreviadas como
     * "1-1-1-1-1", por eso se cubren explícitamente.
     *
     * Condiciones:
     * - Vacío, espacios al inicio o al final, forma abreviada, sin guiones,
     *   caracteres no hexadecimales o con llaves
     *
     * Se espera:
     * - InvalidHotelSearchException con el formato esperado en el mensaje
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "",
            " 550e8400-e29b-41d4-a716-446655440000",
            "550e8400-e29b-41d4-a716-446655440000 ",
            "1-1-1-1-1",
            "550e8400e29b41d4a716446655440000",
            "550e8400-e29b-41d4-a716-44665544000g",
            "{550e8400-e29b-41d4-a716-446655440000}"
    })
    void of_NonCanonicalText_ThrowsInvalidHotelSearch(String text) {
        // given
        String invalidText = text;

        // when
        Executable action = () -> HotelId.of(invalidText);

        // then
        InvalidHotelSearchException exception = assertThrows(InvalidHotelSearchException.class, action,
                "Un hotelId no canónico debería rechazarse");
        assertEquals("hotelId debe ser un UUID canónico de 36 caracteres", exception.getMessage(),
                "El mensaje debería explicar el formato esperado");
    }
}
