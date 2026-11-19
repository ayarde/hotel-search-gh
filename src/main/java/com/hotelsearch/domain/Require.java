package com.hotelsearch.domain;

/**
 * Chequeos de presencia compartidos por los value objects, con un mensaje
 * que nombra el campo afectado.
 */
final class Require {

    private Require() {
    }

    static void nonNull(Object value, String field) {
        if (value == null) {
            throw missing(field);
        }
    }

    static InvalidHotelSearchException missing(String field) {
        return new InvalidHotelSearchException(field + " es obligatorio");
    }
}
