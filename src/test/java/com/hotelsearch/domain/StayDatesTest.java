package com.hotelsearch.domain;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class StayDatesTest {

    /**
     * Verifica que un rango con check-in anterior al check-out se acepta.
     *
     * Condiciones:
     * - Check-out un día después del check-in, el mínimo válido
     *
     * Se espera:
     * - Fechas conservadas sin cambios
     */
    @Test
    void constructor_CheckInBeforeCheckOut_KeepsDates() {
        // given
        LocalDate checkIn = LocalDate.of(2026, 11, 20);
        LocalDate checkOut = checkIn.plusDays(1);

        // when
        StayDates stayDates = new StayDates(checkIn, checkOut);

        // then
        assertAll(
                () -> assertEquals(checkIn, stayDates.checkIn(), "El check-in debería conservarse"),
                () -> assertEquals(checkOut, stayDates.checkOut(), "El check-out debería conservarse"));
    }

    /**
     * Verifica que check-in y check-out no pueden coincidir ni invertirse.
     *
     * Condiciones:
     * - Check-out igual o anterior al check-in
     *
     * Se espera:
     * - InvalidHotelSearchException explicando el orden requerido
     */
    @ParameterizedTest
    @CsvSource({"2026-11-20, 2026-11-20", "2026-11-20, 2026-11-19"})
    void constructor_CheckOutNotAfterCheckIn_ThrowsInvalidHotelSearch(LocalDate checkIn, LocalDate checkOut) {
        // given
        LocalDate invalidCheckOut = checkOut;

        // when
        Executable action = () -> new StayDates(checkIn, invalidCheckOut);

        // then
        InvalidHotelSearchException exception = assertThrows(InvalidHotelSearchException.class, action,
                "Un check-out que no es posterior al check-in debería rechazarse");
        assertEquals("checkIn debe ser anterior a checkOut", exception.getMessage(),
                "El mensaje debería explicar el orden de las fechas");
    }

    /**
     * Verifica que check-in es obligatorio.
     *
     * Condiciones:
     * - Check-in null
     *
     * Se espera:
     * - InvalidHotelSearchException nombrando checkIn
     */
    @Test
    void constructor_NullCheckIn_ThrowsInvalidHotelSearch() {
        // given
        LocalDate checkOut = LocalDate.of(2026, 11, 21);

        // when
        Executable action = () -> new StayDates(null, checkOut);

        // then
        InvalidHotelSearchException exception = assertThrows(InvalidHotelSearchException.class, action,
                "Un check-in null debería rechazarse");
        assertEquals("checkIn es obligatorio", exception.getMessage(), "El mensaje debería nombrar el campo");
    }

    /**
     * Verifica que check-out es obligatorio.
     *
     * Condiciones:
     * - Check-out null
     *
     * Se espera:
     * - InvalidHotelSearchException nombrando checkOut
     */
    @Test
    void constructor_NullCheckOut_ThrowsInvalidHotelSearch() {
        // given
        LocalDate checkIn = LocalDate.of(2026, 11, 20);

        // when
        Executable action = () -> new StayDates(checkIn, null);

        // then
        InvalidHotelSearchException exception = assertThrows(InvalidHotelSearchException.class, action,
                "Un check-out null debería rechazarse");
        assertEquals("checkOut es obligatorio", exception.getMessage(), "El mensaje debería nombrar el campo");
    }

    /**
     * Verifica que la ventana de check-in es inclusiva y usa meses
     * calendario. Cuando el mes vecino es más corto, el límite se ajusta a
     * su último día.
     *
     * Condiciones:
     * - Check-in en ambos límites, en el mismo día y en fines de mes cortos
     *   (incluido un año bisiesto)
     *
     * Se espera:
     * - No se lanza excepción
     */
    @ParameterizedTest
    @CsvSource({
            "2026-11-17, 2026-10-17",
            "2026-11-17, 2026-11-17",
            "2026-11-17, 2026-12-17",
            "2026-03-31, 2026-02-28",
            "2026-03-31, 2026-04-30",
            "2028-03-31, 2028-02-29"
    })
    void requireCheckInWithinMonthOf_CheckInInsideWindow_DoesNotThrow(LocalDate today, LocalDate checkIn) {
        // given
        StayDates stayDates = stayDatesFrom(checkIn);

        // when
        Executable action = () -> stayDates.requireCheckInWithinMonthOf(today);

        // then
        assertDoesNotThrow(action, "Un check-in dentro de la ventana debería aceptarse");
    }

    /**
     * Verifica que se rechaza un check-in un día fuera de cualquiera de los
     * dos límites de la ventana.
     *
     * Condiciones:
     * - Check-in un día antes del límite inferior o un día después del
     *   superior, incluidos fines de mes cortos
     *
     * Se espera:
     * - InvalidHotelSearchException con los límites vigentes en el mensaje
     */
    @ParameterizedTest
    @CsvSource({
            "2026-11-17, 2026-10-16, 2026-10-17, 2026-12-17",
            "2026-11-17, 2026-12-18, 2026-10-17, 2026-12-17",
            "2026-03-31, 2026-02-27, 2026-02-28, 2026-04-30",
            "2026-03-31, 2026-05-01, 2026-02-28, 2026-04-30"
    })
    void requireCheckInWithinMonthOf_CheckInOutsideWindow_ThrowsInvalidHotelSearch(
            LocalDate today, LocalDate checkIn, LocalDate earliest, LocalDate latest) {
        // given
        StayDates stayDates = stayDatesFrom(checkIn);

        // when
        Executable action = () -> stayDates.requireCheckInWithinMonthOf(today);

        // then
        InvalidHotelSearchException exception = assertThrows(InvalidHotelSearchException.class, action,
                "Un check-in fuera de la ventana debería rechazarse");
        assertEquals("checkIn debe estar entre " + earliest + " y " + latest, exception.getMessage(),
                "El mensaje debería indicar los límites de la ventana");
    }

    // <editor-fold desc="Helpers">
    private StayDates stayDatesFrom(LocalDate checkIn) {
        return new StayDates(checkIn, checkIn.plusDays(1));
    }
    // </editor-fold>
}
