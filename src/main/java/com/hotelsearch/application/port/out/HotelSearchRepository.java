package com.hotelsearch.application.port.out;

import com.hotelsearch.domain.HotelSearchCount;
import com.hotelsearch.domain.HotelSearchEvent;
import com.hotelsearch.domain.SearchId;
import java.util.Optional;

/**
 * Almacenamiento de búsquedas aceptadas.
 */
public interface HotelSearchRepository {

    /**
     * Guarda la búsqueda del evento. Un searchId ya existente no es un error:
     * es una redelivery y se descarta.
     *
     * @return {@code true} si se insertó, {@code false} si el searchId ya
     *         existía
     */
    boolean save(HotelSearchEvent event);

    /**
     * Busca la búsqueda junto con la cantidad de otras búsquedas
     * equivalentes, sin contarse a sí misma.
     *
     * @return vacío si el searchId no existe
     */
    Optional<HotelSearchCount> findWithMatchCount(SearchId searchId);
}
