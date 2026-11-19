package com.hotelsearch.infrastructure.output.kafka;

import static com.hotelsearch.HotelSearchFixtures.AGES;
import static com.hotelsearch.HotelSearchFixtures.CHECK_IN;
import static com.hotelsearch.HotelSearchFixtures.CHECK_OUT;
import static com.hotelsearch.HotelSearchFixtures.CREATED_AT;
import static com.hotelsearch.HotelSearchFixtures.HOTEL_ID;
import static com.hotelsearch.HotelSearchFixtures.SEARCH_ID;
import static com.hotelsearch.HotelSearchFixtures.event;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Cubre el armado del payload Kafka a partir del evento de dominio.
 */
class HotelSearchMessageTest {

    /**
     * Verifica que el payload lleva todos los datos del evento, con los
     * identificadores en su forma canónica de texto.
     *
     * Condiciones:
     * - Evento válido
     *
     * Se espera:
     * - searchId y hotelId como UUID en texto
     * - Fechas, edades (en el mismo orden) y createdAt sin cambios
     */
    @Test
    void from_ValidEvent_CopiesEveryField() {
        // given
        var event = event();

        // when
        HotelSearchMessage message = HotelSearchMessage.from(event);

        // then
        assertAll(
                () -> assertEquals(SEARCH_ID.value().toString(), message.searchId(), "searchId inesperado"),
                () -> assertEquals(HOTEL_ID, message.hotelId(), "hotelId inesperado"),
                () -> assertEquals(CHECK_IN, message.checkIn(), "checkIn inesperado"),
                () -> assertEquals(CHECK_OUT, message.checkOut(), "checkOut inesperado"),
                () -> assertEquals(AGES, message.ages(), "ages debería conservar valores y orden"),
                () -> assertEquals(CREATED_AT, message.createdAt(), "createdAt inesperado"));
    }

    /**
     * Verifica que el payload no comparte la lista de edades recibida ni
     * permite modificarla.
     *
     * Condiciones:
     * - Lista de edades mutable al construir el mensaje
     *
     * Se espera:
     * - Cambios posteriores en la lista original no afectan al mensaje
     * - La lista expuesta es inmutable
     */
    @Test
    void constructor_MutableAges_KeepsDefensiveCopy() {
        // given
        List<Integer> ages = new ArrayList<>(AGES);
        HotelSearchMessage message = new HotelSearchMessage(
                SEARCH_ID.value().toString(), HOTEL_ID, CHECK_IN, CHECK_OUT, ages, CREATED_AT);

        // when
        ages.add(99);

        // then
        Executable mutation = () -> message.ages().add(1);
        assertAll(
                () -> assertEquals(AGES, message.ages(), "El mensaje no debería ver cambios de la lista original"),
                () -> assertThrows(UnsupportedOperationException.class, mutation,
                        "La lista de edades del mensaje debería ser inmutable"));
    }
}
