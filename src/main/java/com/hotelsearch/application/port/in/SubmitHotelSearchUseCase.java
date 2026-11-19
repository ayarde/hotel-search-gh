package com.hotelsearch.application.port.in;

import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.SearchId;

/**
 * Alta de una búsqueda nueva.
 */
public interface SubmitHotelSearchUseCase {

    /**
     * Valida la ventana de check-in, asigna un searchId y publica la búsqueda.
     * Retorna después del ACK del broker. Que la búsqueda esté aceptada no
     * significa que ya esté persistida: eso ocurre de forma asincrónica en
     * el consumer.
     *
     * @return el searchId asignado a la búsqueda
     * @throws com.hotelsearch.domain.InvalidHotelSearchException si el
     *         check-in está fuera de la ventana permitida
     * @throws com.hotelsearch.application.port.out.HotelSearchPublicationException
     *         si el broker no confirma
     */
    SearchId submit(HotelSearch search);
}
