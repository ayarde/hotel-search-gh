package com.hotelsearch.application.port.out;

import com.hotelsearch.domain.HotelSearchEvent;

/**
 * Publica búsquedas aceptadas en el broker.
 */
public interface HotelSearchPublisher {

    /**
     * Publica el evento y retorna solo cuando el broker confirmó la recepción.
     *
     * @throws HotelSearchPublicationException si el broker no confirma, por
     *         fallo o por timeout
     */
    void publish(HotelSearchEvent event);
}
