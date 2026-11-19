package com.hotelsearch.application.usecase;

import static com.hotelsearch.HotelSearchFixtures.searchWithCheckIn;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.hotelsearch.application.port.out.HotelSearchPublicationException;
import com.hotelsearch.application.port.out.HotelSearchPublisher;
import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.HotelSearchEvent;
import com.hotelsearch.domain.InvalidHotelSearchException;
import com.hotelsearch.domain.SearchId;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SubmitHotelSearchTest {

    private static final Instant NOW = Instant.parse("2026-11-17T12:00:00Z");

    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

    @Mock
    private HotelSearchPublisher publisher;

    /**
     * Verifica que una búsqueda válida se publica como evento y que el
     * searchId devuelto es el del evento publicado.
     *
     * Condiciones:
     * - Check-in dentro de la ventana según el Clock fijo
     * - Publisher confirma la publicación
     *
     * Se espera:
     * - Un único evento publicado con la búsqueda y createdAt del Clock
     * - searchId UUIDv4 devuelto igual al del evento
     */
    @Test
    void submit_ValidSearch_PublishesEventAndReturnsSearchId() {
        // given
        SubmitHotelSearch submitHotelSearch = new SubmitHotelSearch(publisher, clock);
        HotelSearch search = searchWithCheckIn(LocalDate.of(2026, 11, 20));
        ArgumentCaptor<HotelSearchEvent> published = ArgumentCaptor.forClass(HotelSearchEvent.class);

        // when
        SearchId searchId = submitHotelSearch.submit(search);

        // then
        verify(publisher).publish(published.capture());
        HotelSearchEvent event = published.getValue();
        assertAll(
                () -> assertEquals(searchId, event.searchId(), "El searchId devuelto debería ser el del evento"),
                () -> assertEquals(4, searchId.value().version(), "El searchId debería ser UUIDv4"),
                () -> assertEquals(search, event.search(), "El evento debería llevar la búsqueda recibida"),
                () -> assertEquals(NOW, event.createdAt(), "El createdAt debería salir del Clock"));
    }

    /**
     * Verifica que dos solicitudes iguales generan identificadores distintos:
     * no hay idempotencia HTTP.
     *
     * Condiciones:
     * - La misma búsqueda enviada dos veces
     *
     * Se espera:
     * - Dos publicaciones con searchId distintos
     */
    @Test
    void submit_SameSearchTwice_GeneratesDistinctSearchIds() {
        // given
        SubmitHotelSearch submitHotelSearch = new SubmitHotelSearch(publisher, clock);
        HotelSearch search = searchWithCheckIn(LocalDate.of(2026, 11, 20));

        // when
        SearchId first = submitHotelSearch.submit(search);
        SearchId second = submitHotelSearch.submit(search);

        // then
        assertNotEquals(first, second, "Cada envío debería generar un searchId nuevo");
        verify(publisher, times(2)).publish(any(HotelSearchEvent.class));
    }

    /**
     * Verifica que la ventana de check-in se evalúa con el Clock inyectado y
     * que una búsqueda fuera de ella no llega a publicarse.
     *
     * Condiciones:
     * - Check-in un día después de today + 1 mes
     *
     * Se espera:
     * - InvalidHotelSearchException
     * - Ninguna interacción con el publisher
     */
    @Test
    void submit_CheckInOutsideWindow_ThrowsWithoutPublishing() {
        // given
        SubmitHotelSearch submitHotelSearch = new SubmitHotelSearch(publisher, clock);
        HotelSearch search = searchWithCheckIn(LocalDate.of(2026, 12, 18));

        // when
        Executable action = () -> submitHotelSearch.submit(search);

        // then
        InvalidHotelSearchException exception = assertThrows(InvalidHotelSearchException.class, action,
                "Un check-in fuera de la ventana debería rechazarse");
        assertEquals("checkIn debe estar entre 2026-10-17 y 2026-12-17", exception.getMessage(),
                "El mensaje debería usar la fecha del Clock");
        verifyNoInteractions(publisher);
    }

    /**
     * Verifica que un fallo de publicación no se oculta: sin ACK no hay
     * searchId que devolver.
     *
     * Condiciones:
     * - Publisher lanza HotelSearchPublicationException
     *
     * Se espera:
     * - La misma excepción propagada al llamador
     */
    @Test
    void submit_PublicationFails_PropagatesException() {
        // given
        SubmitHotelSearch submitHotelSearch = new SubmitHotelSearch(publisher, clock);
        HotelSearch search = searchWithCheckIn(LocalDate.of(2026, 11, 20));
        HotelSearchPublicationException failure =
                new HotelSearchPublicationException("Kafka no confirmó la publicación", new IllegalStateException());
        doThrow(failure).when(publisher).publish(any(HotelSearchEvent.class));

        // when
        Executable action = () -> submitHotelSearch.submit(search);

        // then
        HotelSearchPublicationException exception = assertThrows(HotelSearchPublicationException.class, action,
                "El fallo de publicación debería propagarse");
        assertSame(failure, exception, "Debería propagarse la excepción original del publisher");
    }

}
