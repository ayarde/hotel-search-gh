package com.hotelsearch.application.usecase;

import static com.hotelsearch.HotelSearchFixtures.SEARCH_ID;
import static com.hotelsearch.HotelSearchFixtures.search;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.hotelsearch.application.port.in.HotelSearchNotFoundException;
import com.hotelsearch.application.port.out.HotelSearchRepository;
import com.hotelsearch.domain.HotelSearchCount;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CountHotelSearchTest {

    @Mock
    private HotelSearchRepository repository;

    /**
     * Verifica que se devuelve la búsqueda con la cantidad de otras búsquedas
     * equivalentes que informa el repositorio.
     *
     * Condiciones:
     * - El searchId existe y tiene dos búsquedas equivalentes
     *
     * Se espera:
     * - El resultado del repositorio sin cambios
     */
    @Test
    void count_ExistingSearch_ReturnsSearchWithCount() {
        // given
        HotelSearchCount stored = new HotelSearchCount(SEARCH_ID, search(), 2);
        when(repository.findWithMatchCount(SEARCH_ID)).thenReturn(Optional.of(stored));
        CountHotelSearch countHotelSearch = new CountHotelSearch(repository);

        // when
        HotelSearchCount result = countHotelSearch.count(SEARCH_ID);

        // then
        assertEquals(stored, result, "Debería devolverse la búsqueda con su conteo");
    }

    /**
     * Verifica que un searchId inexistente se distingue de un conteo cero.
     *
     * Condiciones:
     * - El repositorio no encuentra el searchId
     *
     * Se espera:
     * - HotelSearchNotFoundException con el searchId en el mensaje
     */
    @Test
    void count_UnknownSearchId_ThrowsNotFound() {
        // given
        when(repository.findWithMatchCount(SEARCH_ID)).thenReturn(Optional.empty());
        CountHotelSearch countHotelSearch = new CountHotelSearch(repository);

        // when
        Executable action = () -> countHotelSearch.count(SEARCH_ID);

        // then
        HotelSearchNotFoundException exception = assertThrows(HotelSearchNotFoundException.class, action,
                "Un searchId inexistente debería informarse como no encontrado");
        assertEquals("No existe una búsqueda con searchId 3f2c8a1e-9b4d-4c6e-8f1a-2b3c4d5e6f70",
                exception.getMessage(), "El mensaje debería incluir el searchId consultado");
    }

}
