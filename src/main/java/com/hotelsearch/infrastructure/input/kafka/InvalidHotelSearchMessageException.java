package com.hotelsearch.infrastructure.input.kafka;

/**
 * El valor del mensaje no se puede leer como una búsqueda: no es JSON, un
 * campo tiene un tipo incorrecto o directamente no hay valor. Reintentar no
 * cambia el resultado, así que el error handler lo envía directo al DLT.
 */
public class InvalidHotelSearchMessageException extends RuntimeException {

    public InvalidHotelSearchMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
