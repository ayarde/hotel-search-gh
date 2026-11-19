package com.hotelsearch.application.usecase;

import com.hotelsearch.application.port.in.PersistHotelSearchUseCase;
import com.hotelsearch.application.port.out.HotelSearchRepository;
import com.hotelsearch.domain.HotelSearchEvent;
import java.util.Objects;
import org.springframework.transaction.annotation.Transactional;

/**
 * Guarda el evento a través del repositorio, sin capturar sus errores.
 *
 * <p>Cada evento se persiste en su propia transacción Oracle. El listener
 * retorna después del commit, y recién entonces se confirma el offset; si la
 * transacción falla, se hace rollback y la excepción llega al listener.
 */
public class PersistHotelSearch implements PersistHotelSearchUseCase {

    private final HotelSearchRepository repository;

    public PersistHotelSearch(HotelSearchRepository repository) {
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    @Override
    @Transactional
    public boolean persist(HotelSearchEvent event) {
        return repository.save(event);
    }
}
