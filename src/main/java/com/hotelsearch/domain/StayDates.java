package com.hotelsearch.domain;

import java.time.LocalDate;
import java.util.Objects;

/**
 * Fechas de entrada y salida de la estadía. El check-in siempre es anterior
 * al check-out.
 */
public record StayDates(LocalDate checkIn, LocalDate checkOut) {

    public StayDates {
        Require.nonNull(checkIn, "checkIn");
        Require.nonNull(checkOut, "checkOut");
        if (!checkIn.isBefore(checkOut)) {
            throw new InvalidHotelSearchException("checkIn debe ser anterior a checkOut");
        }
    }

    /**
     * Exige que el check-in caiga a no más de un mes calendario de
     * {@code today}, en cualquier dirección y con ambos extremos incluidos.
     *
     * <p>Solo aplica al recibir una búsqueda nueva. No forma parte del
     * constructor porque una búsqueda ya aceptada sigue siendo válida aunque
     * la ventana haya quedado atrás.
     */
    public void requireCheckInWithinMonthOf(LocalDate today) {
        Objects.requireNonNull(today, "today");
        LocalDate earliest = today.minusMonths(1);
        LocalDate latest = today.plusMonths(1);
        if (checkIn.isBefore(earliest) || checkIn.isAfter(latest)) {
            throw new InvalidHotelSearchException(
                    "checkIn debe estar entre %s y %s".formatted(earliest, latest));
        }
    }
}
