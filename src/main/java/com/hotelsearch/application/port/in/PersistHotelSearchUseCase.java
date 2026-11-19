package com.hotelsearch.application.port.in;

import com.hotelsearch.domain.HotelSearchEvent;

/**
 * Persistencia de una búsqueda recibida desde Kafka.
 */
public interface PersistHotelSearchUseCase {

    /**
     * Guarda la búsqueda del evento. Los errores de persistencia se propagan
     * para que el consumer decida entre reintentar o enviar al DLT.
     *
     * @return {@code true} si se insertó, {@code false} si era una
     *         redelivery de un searchId ya persistido
     */
    boolean persist(HotelSearchEvent event);
}
