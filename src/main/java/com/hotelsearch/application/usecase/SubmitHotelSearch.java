package com.hotelsearch.application.usecase;

import com.hotelsearch.application.port.in.SubmitHotelSearchUseCase;
import com.hotelsearch.application.port.out.HotelSearchPublisher;
import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.HotelSearchEvent;
import com.hotelsearch.domain.SearchId;
import java.time.Clock;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Evalúa la ventana de check-in con el {@link Clock} inyectado, que también
 * fija el createdAt del evento, y publica la búsqueda.
 */
public class SubmitHotelSearch implements SubmitHotelSearchUseCase {

    private final HotelSearchPublisher publisher;
    private final Clock clock;

    public SubmitHotelSearch(HotelSearchPublisher publisher, Clock clock) {
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public SearchId submit(HotelSearch search) {
        Objects.requireNonNull(search, "search");
        search.stayDates().requireCheckInWithinMonthOf(LocalDate.now(clock));
        HotelSearchEvent event = new HotelSearchEvent(SearchId.generate(), search, clock.instant());
        publisher.publish(event);
        return event.searchId();
    }
}
