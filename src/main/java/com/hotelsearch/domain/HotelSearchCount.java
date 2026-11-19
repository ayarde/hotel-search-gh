package com.hotelsearch.domain;

/**
 * Búsqueda consultada junto con la cantidad de otras búsquedas equivalentes.
 */
public record HotelSearchCount(SearchId searchId, HotelSearch search, long count) {

    public HotelSearchCount {
        Require.nonNull(searchId, "searchId");
        Require.nonNull(search, "search");
    }
}
