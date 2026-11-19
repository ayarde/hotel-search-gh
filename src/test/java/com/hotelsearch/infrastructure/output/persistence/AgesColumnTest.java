package com.hotelsearch.infrastructure.output.persistence;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hotelsearch.domain.Ages;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class AgesColumnTest {

    /**
     * Verifica el formato que se guarda en la columna ages, del que depende la
     * comparación por igualdad de texto en SQL.
     *
     * Condiciones:
     * - Edades con valores de uno y dos dígitos, en un orden no creciente
     *
     * Se espera:
     * - JSON sin espacios y en el mismo orden de la búsqueda
     */
    @Test
    void toJson_SeveralAges_ReturnsCompactJsonInOrder() {
        // given
        Ages ages = new Ages(List.of(29, 28, 4, 3));

        // when
        String json = AgesColumn.toJson(ages);

        // then
        assertEquals("[29,28,4,3]", json, "El JSON debería ser compacto y conservar el orden");
    }

    /**
     * Verifica que el caso más largo admitido por el dominio entra en la
     * columna VARCHAR2(600 CHAR).
     *
     * Condiciones:
     * - 50 edades con el valor entero máximo
     *
     * Se espera:
     * - JSON de 551 caracteres (50 valores de 10 dígitos, 49 comas y dos
     *   corchetes), por debajo del límite de la columna
     */
    @Test
    void toJson_LargestAllowedAges_FitsColumn() {
        // given
        Ages ages = new Ages(Collections.nCopies(50, Integer.MAX_VALUE));

        // when
        String json = AgesColumn.toJson(ages);

        // then
        assertAll(
                () -> assertEquals(551, json.length(), "Largo inesperado para 50 edades máximas"),
                () -> assertTrue(json.length() <= 600, "El JSON no debería superar 600 caracteres"));
    }

    /**
     * Verifica que lo leído de la columna conserva el orden original.
     *
     * Condiciones:
     * - Valor guardado con el formato compacto
     *
     * Se espera:
     * - Lista con las mismas edades y en el mismo orden
     */
    @Test
    void fromJson_CompactJson_ReturnsAgesInOrder() {
        // given
        String json = "[3,4,28,29]";

        // when
        List<Integer> ages = AgesColumn.fromJson(json);

        // then
        assertEquals(List.of(3, 4, 28, 29), ages, "Las edades deberían leerse en el orden guardado");
    }

    /**
     * Verifica que un valor con otro formato no se interpreta a medias.
     *
     * Condiciones:
     * - Valores nulos, vacíos, sin corchetes, con elementos vacíos, con
     *   espacios o no numéricos
     *
     * Se espera:
     * - IllegalStateException que incluye el valor recibido
     */
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"[]", "29,28", "[29,28", "[29,,3]", "[29, 28]", "[a]"})
    void fromJson_UnexpectedFormat_ThrowsIllegalState(String json) {
        // given
        String expectedValue = String.valueOf(json);

        // when
        Executable action = () -> AgesColumn.fromJson(json);

        // then
        IllegalStateException exception = assertThrows(IllegalStateException.class, action,
                "Un formato inesperado debería rechazarse");
        assertEquals("La columna ages tiene un valor inesperado: " + expectedValue, exception.getMessage(),
                "El mensaje debería incluir el valor recibido");
    }

}
