package com.intentmedia.convert;

import org.junit.Test;

import static org.junit.Assert.assertSame;

public class NoOpConverterTest {

    @Test
    public void testConvertReturnsInput() throws Exception {
        // initialize inputs
        String value = "foo";

        // initialize mocks
        // initialize subject
        NoOpConverter<String> subject = new NoOpConverter<String>();

        // invoke target
        // assert
        assertSame(value, subject.convert(value));

        // verify
    }
}
