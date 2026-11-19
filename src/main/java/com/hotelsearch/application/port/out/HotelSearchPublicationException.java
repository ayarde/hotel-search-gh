package com.hotelsearch.application.port.out;

/**
 * El broker no confirmó la publicación de una búsqueda.
 */
public class HotelSearchPublicationException extends RuntimeException {

    public HotelSearchPublicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
