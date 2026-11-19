package com.hotelsearch.application.usecase;

import com.hotelsearch.application.port.in.PersistHotelSearchUseCase;
import com.hotelsearch.application.port.out.HotelSearchRepository;
import com.hotelsearch.domain.HotelSearchEvent;
import java.util.Objects;

/**
 * Guarda el evento a través del repositorio, sin capturar sus errores.
 */
public class PersistHotelSearch implements PersistHotelSearchUseCase {

    private final HotelSearchRepository repository;

    public PersistHotelSearch(HotelSearchRepository repository) {
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    @Override
    public boolean persist(HotelSearchEvent event) {
        return repository.save(event);
    }
}
