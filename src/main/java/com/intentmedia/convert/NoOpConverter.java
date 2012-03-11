package com.intentmedia.convert;

import org.jetbrains.annotations.NotNull;

public class NoOpConverter<F> implements ToPigTypeConverter<F, F> {

    @NotNull
    @Override
    public F convert(@NotNull F value) throws IllegalArgumentException {
        return value;
    }
}
