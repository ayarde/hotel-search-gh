package com.hotelsearch.domain;

/**
 * Indica que una búsqueda, o alguno de sus valores, no cumple las reglas del
 * dominio. El mensaje está pensado para devolverse tal cual al cliente.
 */
public class InvalidHotelSearchException extends RuntimeException {

    public InvalidHotelSearchException(String message) {
        super(message);
    }
}
