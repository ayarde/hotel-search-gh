package com.hotelsearch.application.port.in;

import com.hotelsearch.domain.HotelSearchCount;
import com.hotelsearch.domain.SearchId;

/**
 * Consulta de una búsqueda y de cuántas otras búsquedas equivalentes hay.
 */
public interface CountHotelSearchUseCase {

    /**
     * Recupera la búsqueda y la cantidad de búsquedas equivalentes, sin
     * contarse a sí misma.
     *
     * @return la búsqueda junto con su conteo
     * @throws HotelSearchNotFoundException si el searchId no está persistido
     */
    HotelSearchCount count(SearchId searchId);
}
