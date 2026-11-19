package com.hotelsearch.application.usecase;

import com.hotelsearch.application.port.in.CountHotelSearchUseCase;
import com.hotelsearch.application.port.in.HotelSearchNotFoundException;
import com.hotelsearch.application.port.out.HotelSearchRepository;
import com.hotelsearch.domain.HotelSearchCount;
import com.hotelsearch.domain.SearchId;
import java.util.Objects;

/**
 * Delega la búsqueda y el conteo en el repositorio y traduce la ausencia en
 * {@link HotelSearchNotFoundException}.
 */
public class CountHotelSearch implements CountHotelSearchUseCase {

    private final HotelSearchRepository repository;

    public CountHotelSearch(HotelSearchRepository repository) {
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    @Override
    public HotelSearchCount count(SearchId searchId) {
        return repository.findWithMatchCount(searchId)
                .orElseThrow(() -> new HotelSearchNotFoundException(searchId));
    }
}
