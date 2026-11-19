package com.hotelsearch.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.function.Executable;

/**
 * Aserción compartida para las reglas del dominio: la acción se rechaza con
 * InvalidHotelSearchException y el mensaje exacto que verá el cliente.
 */
final class InvalidHotelSearchAssertions {

    private InvalidHotelSearchAssertions() {
    }

    static void assertInvalid(Executable action, String expectedMessage) {
        InvalidHotelSearchException exception = assertThrows(InvalidHotelSearchException.class, action,
                "El valor inválido debería rechazarse");
        assertEquals(expectedMessage, exception.getMessage(), "El mensaje debería explicar el problema");
    }
}
