package com.hotelsearch.application.usecase;

import static com.hotelsearch.HotelSearchFixtures.event;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hotelsearch.application.port.out.HotelSearchRepository;
import com.hotelsearch.domain.HotelSearchEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PersistHotelSearchTest {

    @Mock
    private HotelSearchRepository repository;

    /**
     * Verifica que un evento nuevo se guarda en el repositorio.
     *
     * Condiciones:
     * - El repositorio inserta la fila
     *
     * Se espera:
     * - save invocado con el evento recibido
     * - Resultado true
     */
    @Test
    void persist_NewEvent_ReturnsTrue() {
        // given
        HotelSearchEvent event = event();
        when(repository.save(event)).thenReturn(true);
        PersistHotelSearch persistHotelSearch = new PersistHotelSearch(repository);

        // when
        boolean inserted = persistHotelSearch.persist(event);

        // then
        assertTrue(inserted, "Un evento nuevo debería informarse como insertado");
        verify(repository).save(event);
    }

    /**
     * Verifica que una redelivery del mismo evento no es un error.
     *
     * Condiciones:
     * - El repositorio informa que el searchId ya existía
     *
     * Se espera:
     * - Resultado false, sin excepción
     */
    @Test
    void persist_DuplicateSearchId_ReturnsFalse() {
        // given
        HotelSearchEvent event = event();
        when(repository.save(event)).thenReturn(false);
        PersistHotelSearch persistHotelSearch = new PersistHotelSearch(repository);

        // when
        boolean inserted = persistHotelSearch.persist(event);

        // then
        assertFalse(inserted, "Un duplicado debería informarse como no insertado");
    }

    /**
     * Verifica que los errores de persistencia no se descartan, para que el
     * consumer pueda reintentar o enviar al DLT.
     *
     * Condiciones:
     * - El repositorio falla al guardar
     *
     * Se espera:
     * - La misma excepción propagada
     */
    @Test
    void persist_RepositoryFails_PropagatesException() {
        // given
        HotelSearchEvent event = event();
        IllegalStateException failure = new IllegalStateException("Oracle no disponible");
        when(repository.save(event)).thenThrow(failure);
        PersistHotelSearch persistHotelSearch = new PersistHotelSearch(repository);

        // when
        Executable action = () -> persistHotelSearch.persist(event);

        // then
        IllegalStateException exception = assertThrows(IllegalStateException.class, action,
                "El error de persistencia debería propagarse");
        assertSame(failure, exception, "Debería propagarse la excepción original");
    }

}
