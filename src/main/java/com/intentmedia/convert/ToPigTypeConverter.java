package com.intentmedia.convert;

import org.jetbrains.annotations.NotNull;

public interface ToPigTypeConverter<F, T> {

    @NotNull
    T convert(@NotNull F value) throws IllegalArgumentException;
}
