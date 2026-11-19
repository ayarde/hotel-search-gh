package com.hotelsearch.domain;

import java.util.UUID;

/**
 * Identificador del hotel buscado.
 */
public record HotelId(UUID value) {

    public HotelId {
        Require.nonNull(value, "hotelId");
    }

    public static HotelId of(String text) {
        return new HotelId(CanonicalUuid.parse(text, "hotelId"));
    }
}
