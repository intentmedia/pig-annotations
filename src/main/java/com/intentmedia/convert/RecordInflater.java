package com.intentmedia.convert;

import org.apache.hadoop.io.Text;
import org.jetbrains.annotations.NotNull;

public interface RecordInflater<T> {

    @NotNull
    T convert(@NotNull Text value) throws IllegalArgumentException;
}
