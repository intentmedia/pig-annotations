package com.intentmedia.convert;

import org.jetbrains.annotations.NotNull;

public class StringFromEnumConverter<E extends Enum<E>> implements ToPigTypeConverter<E, String> {

    @NotNull
    @Override
    public String convert(@NotNull E value) throws IllegalArgumentException {
        return value.name();
    }
}
