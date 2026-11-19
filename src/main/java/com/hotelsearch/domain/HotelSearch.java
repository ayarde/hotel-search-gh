package com.hotelsearch.domain;

import java.time.LocalDate;
import java.util.List;

/**
 * Búsqueda de disponibilidad hotelera. Dos búsquedas son equivalentes cuando
 * coinciden hotel, fechas y edades en el mismo orden.
 */
public record HotelSearch(HotelId hotelId, StayDates stayDates, Ages ages) {

    public HotelSearch {
        Require.nonNull(hotelId, "hotelId");
        Require.nonNull(stayDates, "stayDates");
        Require.nonNull(ages, "ages");
    }

    public static HotelSearch of(String hotelId, LocalDate checkIn, LocalDate checkOut, List<Integer> ages) {
        return new HotelSearch(HotelId.of(hotelId), new StayDates(checkIn, checkOut), new Ages(ages));
    }
}
