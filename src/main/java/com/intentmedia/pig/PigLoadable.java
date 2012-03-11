package com.intentmedia.pig;

import com.intentmedia.convert.RecordInflater;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface PigLoadable {

    Class<? extends RecordInflater<?>> recordInflater();
}
