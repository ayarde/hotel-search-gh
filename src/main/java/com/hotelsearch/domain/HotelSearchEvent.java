package com.hotelsearch.domain;

import java.time.Instant;

/**
 * Búsqueda aceptada, tal como viaja por Kafka hasta Oracle.
 *
 * <p>{@code createdAt} se fija al aceptar la búsqueda, de modo que una
 * redelivery del mismo evento conserva el mismo valor.
 */
public record HotelSearchEvent(SearchId searchId, HotelSearch search, Instant createdAt) {

    public HotelSearchEvent {
        Require.nonNull(searchId, "searchId");
        Require.nonNull(search, "search");
        Require.nonNull(createdAt, "createdAt");
    }
}
