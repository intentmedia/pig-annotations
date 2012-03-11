package com.intentmedia.pig;

import com.intentmedia.convert.NoOpConverter;
import com.intentmedia.convert.ToPigTypeConverter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PigField {

    String name();

    byte type();

    @SuppressWarnings(value = "rawtypes") Class<? extends ToPigTypeConverter> converterClass() default NoOpConverter.class;

    String method() default "";
}
