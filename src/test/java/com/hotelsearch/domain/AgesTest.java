package com.hotelsearch.domain;

import static com.hotelsearch.domain.InvalidHotelSearchAssertions.assertInvalid;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class AgesTest {

    /**
     * Verifica que las edades válidas se conservan en el orden recibido.
     *
     * Condiciones:
     * - Lista con adultos, niños y una edad 0, que es el límite inferior
     *
     * Se espera:
     * - Mismos valores en el mismo orden
     */
    @Test
    void constructor_ValidAges_PreservesOrder() {
        // given
        List<Integer> values = List.of(29, 28, 4, 0);

        // when
        Ages ages = new Ages(values);

        // then
        assertEquals(List.of(29, 28, 4, 0), ages.values(), "Las edades deberían conservar su orden");
    }

    /**
     * Verifica la copia defensiva: cambiar la lista original después de
     * crear Ages no altera sus valores.
     *
     * Condiciones:
     * - Lista mutable modificada tras la construcción
     *
     * Se espera:
     * - Ages mantiene los valores originales
     */
    @Test
    void constructor_SourceListModifiedAfterCreation_KeepsOriginalValues() {
        // given
        List<Integer> source = new ArrayList<>(List.of(29, 28));
        Ages ages = new Ages(source);

        // when
        source.add(5);

        // then
        assertEquals(List.of(29, 28), ages.values(), "Ages no debería reflejar cambios en la lista original");
    }

    /**
     * Verifica que la lista expuesta no puede modificarse.
     *
     * Condiciones:
     * - Intento de agregar un elemento a values()
     *
     * Se espera:
     * - UnsupportedOperationException
     */
    @Test
    void values_ModifyReturnedList_ThrowsUnsupportedOperation() {
        // given
        Ages ages = new Ages(List.of(29, 28));

        // when
        Executable action = () -> ages.values().add(5);

        // then
        assertThrows(UnsupportedOperationException.class, action, "La lista expuesta debería ser inmutable");
    }

    /**
     * Verifica que ages es obligatorio.
     *
     * Condiciones:
     * - Lista null
     *
     * Se espera:
     * - InvalidHotelSearchException nombrando el campo
     */
    @Test
    void constructor_NullList_ThrowsInvalidHotelSearch() {
        // given
        List<Integer> values = null;

        // when
        Executable action = () -> new Ages(values);

        // then
        assertInvalid(action, "ages es obligatorio");
    }

    /**
     * Verifica que la lista de edades no puede estar vacía.
     *
     * Condiciones:
     * - Lista vacía
     *
     * Se espera:
     * - InvalidHotelSearchException indicando que no puede estar vacía
     */
    @Test
    void constructor_EmptyList_ThrowsInvalidHotelSearch() {
        // given
        List<Integer> values = List.of();

        // when
        Executable action = () -> new Ages(values);

        // then
        assertInvalid(action, "ages no puede estar vacío");
    }

    /**
     * Verifica que se aceptan exactamente 50 edades, el máximo permitido.
     *
     * Condiciones:
     * - Lista de 50 elementos
     *
     * Se espera:
     * - Ages con los 50 elementos
     */
    @Test
    void constructor_MaximumSize_KeepsAllAges() {
        // given
        List<Integer> values = Collections.nCopies(Ages.MAX_SIZE, 30);

        // when
        Ages ages = new Ages(values);

        // then
        assertEquals(50, ages.values().size(), "Deberían aceptarse 50 edades");
    }

    /**
     * Verifica que se rechazan listas que superan el máximo.
     *
     * Condiciones:
     * - Lista de 51 elementos
     *
     * Se espera:
     * - InvalidHotelSearchException indicando el máximo
     */
    @Test
    void constructor_AboveMaximumSize_ThrowsInvalidHotelSearch() {
        // given
        List<Integer> values = Collections.nCopies(Ages.MAX_SIZE + 1, 30);

        // when
        Executable action = () -> new Ages(values);

        // then
        assertInvalid(action, "ages admite como máximo 50 elementos");
    }

    /**
     * Verifica que ninguna edad puede ser null y que el mensaje señala la
     * posición.
     *
     * Condiciones:
     * - Null en la segunda posición
     *
     * Se espera:
     * - InvalidHotelSearchException con el índice afectado
     */
    @Test
    void constructor_NullElement_ThrowsInvalidHotelSearch() {
        // given
        List<Integer> values = Arrays.asList(29, null, 4);

        // when
        Executable action = () -> new Ages(values);

        // then
        assertInvalid(action, "ages[1] es obligatorio");
    }

    /**
     * Verifica que las edades negativas se rechazan.
     *
     * Condiciones:
     * - Edad -1 en la tercera posición
     *
     * Se espera:
     * - InvalidHotelSearchException con el índice afectado
     */
    @Test
    void constructor_NegativeAge_ThrowsInvalidHotelSearch() {
        // given
        List<Integer> values = List.of(29, 28, -1);

        // when
        Executable action = () -> new Ages(values);

        // then
        assertInvalid(action, "ages[2] debe ser mayor o igual a 0");
    }

    /**
     * Verifica que el orden forma parte de la identidad de las edades.
     *
     * Condiciones:
     * - Mismas edades en distinto orden, y en el mismo orden
     *
     * Se espera:
     * - Distinto orden no es igual; mismo orden es igual con el mismo hashCode
     */
    @Test
    void equals_AgesOrder_DeterminesEquality() {
        // given
        Ages ages = new Ages(List.of(29, 28, 4));

        // when
        Ages sameOrder = new Ages(List.of(29, 28, 4));
        Ages otherOrder = new Ages(List.of(4, 28, 29));

        // then
        assertAll(
                () -> assertEquals(ages, sameOrder, "Mismo orden debería ser igual"),
                () -> assertEquals(ages.hashCode(), sameOrder.hashCode(), "Mismo orden debería tener el mismo hashCode"),
                () -> assertNotEquals(ages, otherOrder, "Distinto orden no debería ser igual"));
    }
}
