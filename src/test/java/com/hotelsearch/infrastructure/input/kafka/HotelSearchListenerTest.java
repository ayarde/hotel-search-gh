package com.hotelsearch.infrastructure.input.kafka;

import static com.hotelsearch.HotelSearchFixtures.event;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.hotelsearch.application.port.in.PersistHotelSearchUseCase;
import com.hotelsearch.domain.HotelSearch;
import com.hotelsearch.domain.HotelSearchEvent;
import com.hotelsearch.domain.InvalidHotelSearchException;
import com.hotelsearch.domain.SearchId;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class HotelSearchListenerTest {

    private static final String VALID_MESSAGE = """
            {"searchId":"3f2c8a1e-9b4d-4c6e-8f1a-2b3c4d5e6f70",\
            "hotelId":"550e8400-e29b-41d4-a716-446655440000",\
            "checkIn":"2026-11-09","checkOut":"2026-11-29",\
            "ages":[29,28,4,3],"createdAt":"2026-11-17T12:00:00Z"}""";

    @Mock
    private PersistHotelSearchUseCase persistHotelSearch;

    private HotelSearchListener listener;

    @BeforeEach
    void createListener() {
        listener = new HotelSearchListener(persistHotelSearch, JsonMapper.shared());
    }

    /**
     * Verifica que un mensaje válido se traduce al evento de dominio y se
     * persiste.
     *
     * Condiciones:
     * - JSON con el formato que publica el producer
     *
     * Se espera:
     * - persist invocado con el evento equivalente al publicado
     */
    @Test
    void onMessage_ValidMessage_PersistsEvent() {
        // given
        when(persistHotelSearch.persist(any())).thenReturn(true);

        // when
        listener.onMessage(VALID_MESSAGE.getBytes(UTF_8));

        // then
        verify(persistHotelSearch).persist(event());
    }

    /**
     * Verifica que una redelivery ya persistida termina sin error, para que
     * el offset se confirme y el mensaje no vuelva a procesarse.
     *
     * Condiciones:
     * - El caso de uso informa que el searchId ya existía
     *
     * Se espera:
     * - El listener retorna normalmente
     */
    @Test
    void onMessage_RedeliveredMessage_CompletesNormally() {
        // given
        when(persistHotelSearch.persist(event())).thenReturn(false);

        // when
        listener.onMessage(VALID_MESSAGE.getBytes(UTF_8));

        // then
        verify(persistHotelSearch).persist(event());
    }

    /**
     * Verifica que el consumer no vuelve a aplicar la ventana de check-in:
     * esa regla se evalúa al aceptar la búsqueda, no al persistirla.
     *
     * Condiciones:
     * - Check-in de hace años
     *
     * Se espera:
     * - El evento se persiste igual
     */
    @Test
    void onMessage_PastCheckIn_PersistsEvent() {
        // given
        String message = VALID_MESSAGE.replace("2026-11-09", "2020-01-01");
        HotelSearchEvent expected = new HotelSearchEvent(
                SearchId.of("3f2c8a1e-9b4d-4c6e-8f1a-2b3c4d5e6f70"),
                HotelSearch.of("550e8400-e29b-41d4-a716-446655440000",
                        LocalDate.of(2020, 1, 1), LocalDate.of(2026, 11, 29), List.of(29, 28, 4, 3)),
                Instant.parse("2026-11-17T12:00:00Z"));
        when(persistHotelSearch.persist(any())).thenReturn(true);

        // when
        listener.onMessage(message.getBytes(UTF_8));

        // then
        verify(persistHotelSearch).persist(expected);
    }

    /**
     * Verifica que una propiedad desconocida no rompe el consumo, para que el
     * producer pueda agregar campos sin coordinar el despliegue.
     *
     * Condiciones:
     * - JSON válido con una propiedad extra
     *
     * Se espera:
     * - El evento se persiste
     */
    @Test
    void onMessage_UnknownProperty_PersistsEvent() {
        // given
        String message = VALID_MESSAGE.replace("{", "{\"source\":\"web\",");
        when(persistHotelSearch.persist(any())).thenReturn(true);

        // when
        listener.onMessage(message.getBytes(UTF_8));

        // then
        verify(persistHotelSearch).persist(event());
    }

    /**
     * Verifica que un payload que no es JSON se rechaza como mensaje
     * ilegible, que el error handler envía directo al DLT.
     *
     * Condiciones:
     * - Bytes que no forman un JSON válido
     *
     * Se espera:
     * - InvalidHotelSearchMessageException
     * - El caso de uso no se invoca
     */
    @Test
    void onMessage_MalformedJson_ThrowsInvalidMessage() {
        // given
        byte[] payload = "{\"searchId\":".getBytes(UTF_8);

        // when
        Executable action = () -> listener.onMessage(payload);

        // then
        assertThrows(InvalidHotelSearchMessageException.class, action,
                "Un JSON mal formado debería rechazarse como mensaje ilegible");
        verifyNoInteractions(persistHotelSearch);
    }

    /**
     * Verifica que un tipo de dato incorrecto se rechaza como mensaje
     * ilegible.
     *
     * Condiciones:
     * - checkIn con una fecha inexistente
     *
     * Se espera:
     * - InvalidHotelSearchMessageException
     * - El caso de uso no se invoca
     */
    @Test
    void onMessage_InvalidDate_ThrowsInvalidMessage() {
        // given
        byte[] payload = VALID_MESSAGE.replace("2026-11-09", "2026-13-45").getBytes(UTF_8);

        // when
        Executable action = () -> listener.onMessage(payload);

        // then
        assertThrows(InvalidHotelSearchMessageException.class, action,
                "Una fecha inválida debería rechazarse como mensaje ilegible");
        verifyNoInteractions(persistHotelSearch);
    }

    /**
     * Verifica que un mensaje sin valor (tombstone) o con el literal null se
     * rechaza en lugar de fallar con NullPointerException.
     *
     * Condiciones:
     * - Payload null y payload "null"
     *
     * Se espera:
     * - InvalidHotelSearchMessageException en ambos casos
     * - El caso de uso no se invoca
     */
    @Test
    void onMessage_EmptyPayload_ThrowsInvalidMessage() {
        // given
        byte[] jsonNull = "null".getBytes(UTF_8);

        // when
        Executable withoutValue = () -> listener.onMessage(null);
        Executable withJsonNull = () -> listener.onMessage(jsonNull);

        // then
        assertAll(
                () -> assertThrows(InvalidHotelSearchMessageException.class, withoutValue,
                        "Un mensaje sin valor debería rechazarse"),
                () -> assertThrows(InvalidHotelSearchMessageException.class, withJsonNull,
                        "El literal null debería rechazarse"));
        verifyNoInteractions(persistHotelSearch);
    }

    /**
     * Verifica que el consumer valida la estructura del evento con las mismas
     * reglas del dominio.
     *
     * Condiciones:
     * - Mensajes sin searchId, con una edad negativa y con una edad null
     *
     * Se espera:
     * - InvalidHotelSearchException con el campo afectado
     * - El caso de uso no se invoca
     */
    @Test
    void onMessage_InvalidStructure_ThrowsInvalidHotelSearch() {
        // given
        byte[] withoutSearchId = VALID_MESSAGE
                .replace("\"searchId\":\"3f2c8a1e-9b4d-4c6e-8f1a-2b3c4d5e6f70\",", "").getBytes(UTF_8);
        byte[] negativeAge = VALID_MESSAGE.replace("[29,28,4,3]", "[29,-1]").getBytes(UTF_8);
        byte[] nullAge = VALID_MESSAGE.replace("[29,28,4,3]", "[29,null]").getBytes(UTF_8);

        // when
        Executable missingId = () -> listener.onMessage(withoutSearchId);
        Executable invalidAge = () -> listener.onMessage(negativeAge);
        Executable missingAge = () -> listener.onMessage(nullAge);

        // then
        assertAll(
                () -> assertEquals("searchId es obligatorio",
                        assertThrows(InvalidHotelSearchException.class, missingId).getMessage(),
                        "Debería informar el searchId faltante"),
                () -> assertThrows(InvalidHotelSearchException.class, invalidAge,
                        "Una edad negativa debería rechazarse"),
                () -> assertThrows(InvalidHotelSearchException.class, missingAge,
                        "Una edad null debería rechazarse"));
        verifyNoInteractions(persistHotelSearch);
    }

    /**
     * Verifica que un mensaje legible pero incompleto o con un UUID mal
     * escrito falla con InvalidHotelSearchException, que el error handler
     * envía directo al DLT. Otra excepción (NullPointerException,
     * IllegalArgumentException) se reintentaría como si fuera transitoria.
     *
     * Condiciones:
     * - searchId que no es un UUID canónico
     * - Sin hotelId, sin checkIn, sin checkOut y sin createdAt
     *
     * Se espera:
     * - InvalidHotelSearchException con el campo afectado en cada caso
     * - El caso de uso no se invoca
     */
    @Test
    void onMessage_IncompleteOrMalformedFields_ThrowsInvalidHotelSearch() {
        // given
        Map<String, byte[]> payloads = new LinkedHashMap<>();
        payloads.put("searchId debe ser un UUID canónico de 36 caracteres", VALID_MESSAGE
                .replace("3f2c8a1e-9b4d-4c6e-8f1a-2b3c4d5e6f70", "3f2c8a1e").getBytes(UTF_8));
        payloads.put("hotelId es obligatorio", VALID_MESSAGE
                .replace("\"hotelId\":\"550e8400-e29b-41d4-a716-446655440000\",", "").getBytes(UTF_8));
        payloads.put("checkIn es obligatorio", VALID_MESSAGE
                .replace("\"checkIn\":\"2026-11-09\",", "").getBytes(UTF_8));
        payloads.put("checkOut es obligatorio", VALID_MESSAGE
                .replace(",\"checkOut\":\"2026-11-29\"", "").getBytes(UTF_8));
        payloads.put("createdAt es obligatorio", VALID_MESSAGE
                .replace(",\"createdAt\":\"2026-11-17T12:00:00Z\"", "").getBytes(UTF_8));

        // when
        Map<String, Executable> actions = new LinkedHashMap<>();
        payloads.forEach((expectedMessage, payload) -> actions.put(expectedMessage, () -> listener.onMessage(payload)));

        // then
        assertAll(actions.entrySet().stream().map(entry -> () -> assertEquals(entry.getKey(),
                assertThrows(InvalidHotelSearchException.class, entry.getValue(),
                        "Debería rechazarse como búsqueda inválida: " + entry.getKey()).getMessage(),
                "Debería informar el campo afectado")));
        verifyNoInteractions(persistHotelSearch);
    }

    /**
     * Verifica que el listener no oculta errores de persistencia: el error
     * handler decide si reintentar o enviar al DLT, y el offset no se
     * confirma.
     *
     * Condiciones:
     * - El caso de uso falla
     *
     * Se espera:
     * - La misma excepción propagada
     */
    @Test
    void onMessage_PersistenceFails_PropagatesException() {
        // given
        IllegalStateException failure = new IllegalStateException("Oracle no disponible");
        when(persistHotelSearch.persist(any())).thenThrow(failure);

        // when
        Executable action = () -> listener.onMessage(VALID_MESSAGE.getBytes(UTF_8));

        // then
        IllegalStateException exception = assertThrows(IllegalStateException.class, action,
                "El error de persistencia debería propagarse");
        assertSame(failure, exception, "Debería propagarse la excepción original");
    }
}
