package com.intentmedia.pig;

import com.intentmedia.convert.RecordInflater;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class PigAssertHelper {

    private PigAssertHelper() {
    }

    public static void assertTupleValueAndType(Object expectedValue, Tuple tuple, byte expectedType, int index)
            throws ExecException {

        assertTupleValue(tuple, expectedType, index);
        assertTupleType(expectedValue, tuple, index);
    }

    public static void assertBooleanTupleValueAndTypeIsInteger(Boolean expectedValue, Tuple tuple, int index) throws ExecException {
        Integer expected = expectedValue ? 1 : 0;

        assertTupleValueAndType(expected, tuple, DataType.INTEGER, index);
    }

    public static void assertResourceFieldSchemaNameAndType(String expectedFieldName,
                                                            byte expectedType,
                                                            ResourceSchema.ResourceFieldSchema resourceFieldSchema) {

        assertResourceFieldSchemaName(expectedFieldName, resourceFieldSchema);
        assertResourceFieldSchemaType(expectedType, resourceFieldSchema);
    }

    public static void assertBooleanResourceFieldSchemaNameAndTypeIsRepresentedAsInteger(String expectedFieldName,
                                                                                         ResourceSchema.ResourceFieldSchema resourceFieldSchema) {
        assertResourceFieldSchemaName(expectedFieldName, resourceFieldSchema);
        assertResourceFieldSchemaType(DataType.INTEGER, resourceFieldSchema);
    }

    public static void assertNonPigFields(Class<?> clazz,
                                          String[] expectedNonPigFieldNames,
                                          ResourceSchema.ResourceFieldSchema[] pigFields,
                                          int numberOfEmbeddedPigFields,
                                          int numberOfSuperClassPigFields) {

        Field[] declaredFields = clazz.getDeclaredFields();
        List<Field> actualNonPigFields = new ArrayList<Field>();

        for (Field field : declaredFields) {
            if (!field.isAnnotationPresent(PigField.class)) {
                actualNonPigFields.add(field);
            }
        }

        for (int i = 0; i < actualNonPigFields.size(); i++) {
            String expectedNonPigFieldName = expectedNonPigFieldNames[i];
            String actualNonPigFieldName = actualNonPigFields.get(i).getName();

            assertEquals(
                    String.format(
                            "Expected non-%s %d to have name [%s] but was [%s]",
                            PigField.class.getSimpleName(),
                            i,
                            expectedNonPigFieldName,
                            actualNonPigFieldName),
                    expectedNonPigFieldName,
                    actualNonPigFieldName);
        }

        assertEquals(
                String.format(
                        "Expected number of pig fields to be the sum of:\n" +
                                "\tNumber of pig fields expanded from embedded objects: [%d\n" +
                                "\tNumber of pig fields from super class: [%d]\n" +
                                "\tNumber of pig fields on direct object: [%d]\n" +
                                "Expected [%d] but got [%d]",
                        numberOfEmbeddedPigFields,
                        numberOfSuperClassPigFields,
                        declaredFields.length - expectedNonPigFieldNames.length,
                        declaredFields.length - expectedNonPigFieldNames.length + numberOfEmbeddedPigFields + numberOfSuperClassPigFields,
                        pigFields.length
                ),
                declaredFields.length - expectedNonPigFieldNames.length + numberOfEmbeddedPigFields + numberOfSuperClassPigFields,
                pigFields.length);
    }

    public static void assertTupleSlice(Tuple expected, Tuple returnValue, int startIndex) throws ExecException {
        for (int i = 0; i < expected.size(); i++) {
            Object expectedTupleValue = expected.get(i);
            Object actual = returnValue.get(startIndex + i);

            assertEquals(
                    String.format("Tuple value [%d] should be [%s] but got [%s]", startIndex + i, expectedTupleValue, actual),
                    expectedTupleValue,
                    actual);
        }
    }

    public static void assertResourceFieldSchemaSlice(ResourceSchema.ResourceFieldSchema[] expected,
                                                      ResourceSchema.ResourceFieldSchema[] returnValue,
                                                      int startIndex) {
        for (int i = 0; i < expected.length; i++) {
            String expectedValue = expected[i].toString();
            String actualValue = returnValue[startIndex + i].toString();

            assertEquals(
                    String.format("%s value [%d] should be [%s] but got [%s]",
                            ResourceSchema.ResourceFieldSchema.class.getSimpleName(),
                            i,
                            expectedValue,
                            actualValue),
                    expectedValue,
                    actualValue);
        }
    }

    public static void assertDataLoader(Text stubbedRecord, Class<?> clazz, RecordInflater<?> recordInflater)
            throws Exception {

        // initialize inputs
        Job job = new Job();
        Tuple expectedTuple =
                new PigAnnotationHelper(clazz).toTuple(recordInflater.convert(stubbedRecord), TupleFactory.getInstance());

        // initialize mocks
        // initialize subject
        AnnotatedObjectLoader subject = new AnnotatedObjectLoader(clazz.getName());
        subject.setLocation("/intentmedia-hadoop-test/input/", job);
        subject.prepareToRead(new StubRecordReader(stubbedRecord), null);

        Tuple returnValue = subject.getNext();

        // invoke target
        // assert
        assertEquals(expectedTuple, returnValue);

        // verify
    }

    private static void assertTupleType(Object expectedValue, Tuple tuple, int index) throws ExecException {
        Object actual = tuple.get(index);

        assertEquals(
                String.format("Object in %s should be [%s] but got [%s]", Tuple.class.getSimpleName(), expectedValue, actual),
                expectedValue,
                actual);
    }

    private static void assertTupleValue(Tuple tuple, byte expectedType, int index) throws ExecException {
        byte type = tuple.getType(index);

        assertEquals(
                String.format("Object in %s should be type [%s] but got [%s]",
                        Tuple.class.getSimpleName(),
                        DataType.findTypeName(expectedType),
                        DataType.findTypeName(type)), expectedType, type);
    }

    private static void assertResourceFieldSchemaType(byte expectedType, ResourceSchema.ResourceFieldSchema resourceFieldSchema) {
        byte actualType = resourceFieldSchema.getType();
        assertEquals(
                String.format("%s should have type [%s] but got [%s]",
                        ResourceSchema.ResourceFieldSchema.class.getSimpleName(),
                        DataType.findTypeName(expectedType),
                        DataType.findTypeName(actualType)),
                expectedType,
                actualType);
    }

    private static void assertResourceFieldSchemaName(String expectedFieldName,
                                                      ResourceSchema.ResourceFieldSchema resourceFieldSchema) {

        String actualName = resourceFieldSchema.getName();
        assertEquals(
                String.format("%s should have name [%s] but got [%s]",
                        ResourceSchema.ResourceFieldSchema.class.getSimpleName(),
                        expectedFieldName,
                        actualName),
                expectedFieldName,
                actualName);
    }
}
