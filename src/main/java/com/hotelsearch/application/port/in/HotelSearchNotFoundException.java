package com.hotelsearch.application.port.in;

import com.hotelsearch.domain.SearchId;

/**
 * No existe una búsqueda persistida con el searchId consultado. Puede ser un
 * id desconocido o uno aceptado que el consumer todavía no persistió.
 */
public class HotelSearchNotFoundException extends RuntimeException {

    public HotelSearchNotFoundException(SearchId searchId) {
        super("No existe una búsqueda con searchId " + searchId.value());
    }
}
