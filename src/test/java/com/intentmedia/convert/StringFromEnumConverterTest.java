package com.intentmedia.convert;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringFromEnumConverterTest {

    @Test
    public void testConvert() throws Exception {
        // initialize inputs
        // initialize mocks
        // initialize subject

        // invoke target
        // assert
        assertEquals("A", new StringFromEnumConverter<TestType>().convert(TestType.A));
        assertEquals("B", new StringFromEnumConverter<TestType>().convert(TestType.B));

        // verify
    }

    private static enum TestType {A, B}
}
