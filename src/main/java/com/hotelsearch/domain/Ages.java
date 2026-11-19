package com.hotelsearch.domain;

import java.util.List;

/**
 * Edades de los huéspedes. El orden forma parte de la identidad: las mismas
 * edades en otro orden son otra búsqueda.
 */
public record Ages(List<Integer> values) {

    static final int MAX_SIZE = 50;

    public Ages {
        Require.nonNull(values, "ages");
        // Se valida sobre una copia para que nadie cambie la lista entre la
        // validación y la construcción.
        Integer[] snapshot = values.toArray(Integer[]::new);
        if (snapshot.length == 0) {
            throw new InvalidHotelSearchException("ages no puede estar vacío");
        }
        if (snapshot.length > MAX_SIZE) {
            throw new InvalidHotelSearchException("ages admite como máximo %d elementos".formatted(MAX_SIZE));
        }
        for (int i = 0; i < snapshot.length; i++) {
            if (snapshot[i] == null) {
                throw Require.missing("ages[%d]".formatted(i));
            }
            if (snapshot[i] < 0) {
                throw new InvalidHotelSearchException("ages[%d] debe ser mayor o igual a 0".formatted(i));
            }
        }
        values = List.of(snapshot);
    }
}
