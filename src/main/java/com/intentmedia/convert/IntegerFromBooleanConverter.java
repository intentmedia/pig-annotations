package com.intentmedia.convert;

import org.jetbrains.annotations.NotNull;

public class IntegerFromBooleanConverter implements ToPigTypeConverter<Boolean, Integer> {

    @NotNull
    @Override
    public Integer convert(@NotNull Boolean value) throws IllegalArgumentException {
        return value == Boolean.TRUE ? 1 : 0;
    }
}
