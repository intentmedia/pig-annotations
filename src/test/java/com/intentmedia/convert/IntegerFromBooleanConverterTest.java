package com.intentmedia.convert;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IntegerFromBooleanConverterTest {

    @Test
    public void testConvert() throws Exception {
        // initialize inputs
        // initialize mocks
        // initialize subject
        IntegerFromBooleanConverter subject = new IntegerFromBooleanConverter();

        // invoke target

        // assert
        assertEquals(Integer.valueOf(1), subject.convert(Boolean.TRUE));
        assertEquals(Integer.valueOf(0), subject.convert(Boolean.FALSE));

        // verify
    }
}
