package com.intentmedia.pig;

import com.intentmedia.convert.RecordInflater;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMockSupport;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class AnnotatedObjectLoaderTest extends EasyMockSupport {

    @Test(expected = RuntimeException.class)
    public void testAnnotatedJsonLoaderDoesNotSupportClassesWithoutPigLoadableAnnotation() throws Exception {
        // initialize inputs
        // initialize mocks
        // initialize subject

        // invoke target
        new AnnotatedObjectLoader(NotPigLoadableStub.class.getName());

        // assert
        // verify
    }

    @Test
    public void testGetPartitionKeys() throws Exception {
        // initialize inputs
        // initialize mocks
        // initialize subject
        AnnotatedObjectLoader subject = new AnnotatedObjectLoader(PigFieldAnnotatedStub.class.getName());

        // invoke target
        String[] returnValue = subject.getPartitionKeys(null, null);

        // assert
        assertNull(returnValue);

        // verify
    }

    public static final class DeserializerStub implements RecordInflater<Text> {

        @NotNull
        @Override
        public Text convert(@NotNull Text value) throws IllegalArgumentException {
            return null;
        }
    }

    @PigLoadable(recordInflater = DeserializerStub.class)
    private static final class PigFieldAnnotatedStub {

    }

    private static final class NotPigLoadableStub {

    }
}

