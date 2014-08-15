package com.intentmedia.pig;

import com.intentmedia.convert.IntegerFromBooleanConverter;
import com.intentmedia.convert.StringFromEnumConverter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import javax.persistence.Embedded;

import static com.intentmedia.pig.PigAssertHelper.assertBooleanResourceFieldSchemaNameAndTypeIsRepresentedAsInteger;
import static com.intentmedia.pig.PigAssertHelper.assertResourceFieldSchemaNameAndType;
import static com.intentmedia.pig.PigAssertHelper.assertTupleValueAndType;

public class PigAnnotationHelperTest {

    @Test
    public void testGetResourceSchema() throws Exception {
        // initialize inputs
        // initialize mocks
        // initialize subject
        PigAnnotationHelper subject = new PigAnnotationHelper(PigFieldAnnotatedStub.class);

        // invoke target
        ResourceSchema returnValue = subject.getResourceSchema();

        // assert
        ResourceSchema.ResourceFieldSchema[] fields = returnValue.getFields();
        assertResourceFieldSchemaNameAndType("string_field", DataType.CHARARRAY, fields[0]);
        assertResourceFieldSchemaNameAndType("integer_field", DataType.INTEGER, fields[1]);
        assertBooleanResourceFieldSchemaNameAndTypeIsRepresentedAsInteger("boolean_field", fields[2]);
        assertResourceFieldSchemaNameAndType("enum_field", DataType.CHARARRAY, fields[3]);

        // verify
    }

    @Test
    public void testGetResourceSchemaIncludesFieldsFromSuperClasses() throws Exception {
        // initialize inputs
        // initialize mocks
        // initialize subject
        PigAnnotationHelper subject = new PigAnnotationHelper(PigFieldAnnotatedStub.class);

        // invoke target
        ResourceSchema returnValue = subject.getResourceSchema();

        // assert
        ResourceSchema.ResourceFieldSchema[] fields = returnValue.getFields();
        assertResourceFieldSchemaNameAndType("parent_string_field", DataType.CHARARRAY, fields[6]);
        assertResourceFieldSchemaNameAndType("grand_parent_string_field", DataType.CHARARRAY, fields[7]);

        // verify
    }

    @Test
    public void testGetResourceSchemaIncludesEmbeddedFields() throws Exception {
        // initialize inputs
        // initialize mocks
        // initialize subject
        PigAnnotationHelper subject = new PigAnnotationHelper(PigFieldAnnotatedStub.class);

        // invoke target
        ResourceSchema returnValue = subject.getResourceSchema();

        // assert
        ResourceSchema.ResourceFieldSchema[] fields = returnValue.getFields();
        assertResourceFieldSchemaNameAndType("embedded_string_field", DataType.CHARARRAY, fields[5]);

        // verify
    }

    @Test
    public void testToTuple() throws Exception {
        // initialize inputs
        String stringField = "aString";
        Integer integerField = 42;
        Boolean booleanField = Boolean.TRUE;

        PigFieldAnnotatedStub stub = new PigFieldAnnotatedStub(stringField, integerField, booleanField, null, null);

        // initialize mocks
        // initialize subject
        PigAnnotationHelper subject = new PigAnnotationHelper(PigFieldAnnotatedStub.class);

        // invoke target
        Tuple returnValue = subject.toTuple(stub, TupleFactory.getInstance());

        // assert
        assertTupleValueAndType(stringField, returnValue, DataType.CHARARRAY, 0);
        assertTupleValueAndType(integerField, returnValue, DataType.INTEGER, 1);
        com.intentmedia.pig.PigAssertHelper.assertBooleanTupleValueAndTypeIsInteger(booleanField, returnValue, 2);

        // verify
    }

    @Test
    public void testToTupleIncludesFieldsFromSuperClasses() throws Exception {
        // initialize inputs
        String parentStringField = "parentString";
        String grandParentStringField = "grandParentString";
        PigFieldAnnotatedStub stub = new PigFieldAnnotatedStub(null, null, null, null, null);
        stub.setParentStringField(parentStringField);
        stub.setGrandParentStringField(grandParentStringField);

        // initialize mocks
        // initialize subject
        PigAnnotationHelper subject = new PigAnnotationHelper(PigFieldAnnotatedStub.class);

        // invoke target
        Tuple returnValue = subject.toTuple(stub, TupleFactory.getInstance());

        // assert
        assertTupleValueAndType(parentStringField, returnValue, DataType.CHARARRAY, 6);
        assertTupleValueAndType(grandParentStringField, returnValue, DataType.CHARARRAY, 7);

        // verify
    }

    @Test
    public void testToTupleConvertsEnumsToStrings() throws Exception {
        // initialize inputs
        TestType enumField = TestType.A;

        PigFieldAnnotatedStub stub = new PigFieldAnnotatedStub(null, null, null, null, enumField);

        // initialize mocks
        // initialize subject
        PigAnnotationHelper subject = new PigAnnotationHelper(PigFieldAnnotatedStub.class);

        // invoke target
        Tuple returnValue = subject.toTuple(stub, TupleFactory.getInstance());

        // assert
        assertTupleValueAndType(enumField.name(), returnValue, DataType.CHARARRAY, 3);

        // verify
    }

    @Test
    public void testToTupleAppendsFieldsFromEmbeddedObjects() throws Exception {
        // initialize inputs
        String embeddedStringField = "an_embedded_string";
        EmbeddedPigFieldAnnotatedStub embeddedStub = new EmbeddedPigFieldAnnotatedStub(embeddedStringField);

        PigFieldAnnotatedStub stub = new PigFieldAnnotatedStub(null, null, null, embeddedStub, null);

        // initialize mocks
        // initialize subject
        PigAnnotationHelper subject = new PigAnnotationHelper(PigFieldAnnotatedStub.class);

        // invoke target
        Tuple returnValue = subject.toTuple(stub, TupleFactory.getInstance());

        // assert
        assertTupleValueAndType(embeddedStringField, returnValue, DataType.CHARARRAY, 5);

        // verify
    }

    @Test
    public void testToTupleDelegatesToMethodIfSpecified() throws Exception {
        // initialize inputs
        PigFieldAnnotatedStub stub = new PigFieldAnnotatedStub(null, null, null, null, null);

        // initialize mocks
        // initialize subject
        PigAnnotationHelper subject = new PigAnnotationHelper(PigFieldAnnotatedStub.class);

        // invoke target
        Tuple returnValue = subject.toTuple(stub, TupleFactory.getInstance());

        // assert
        assertTupleValueAndType("property string", returnValue, DataType.CHARARRAY, 4);
        // verify
    }

    private static class PigFieldAnnotatedStubParent extends PigFieldAnnotatedStubGrandParent {

        @PigField(name = "parent_string_field", type = DataType.CHARARRAY)
        private String parentStringField;

        public void setParentStringField(String parentStringField) {
            this.parentStringField = parentStringField;
        }
    }

    private static class PigFieldAnnotatedStubGrandParent {

        @PigField(name = "grand_parent_string_field", type = DataType.CHARARRAY)
        private String grandParentStringField;

        public void setGrandParentStringField(String grandParentStringField) {
            this.grandParentStringField = grandParentStringField;
        }
    }

    private static class PigFieldAnnotatedStub extends PigFieldAnnotatedStubParent {

        @PigField(name = "string_field", type = DataType.CHARARRAY)
        private String stringField;

        @PigField(name = "integer_field", type = DataType.INTEGER)
        private Integer integerField;

        @PigField(name = "boolean_field", type = DataType.INTEGER, converterClass = IntegerFromBooleanConverter.class)
        private Boolean booleanField;

        @Embedded
        private EmbeddedPigFieldAnnotatedStub embeddedPigFieldAnnotatedStub;

        @PigField(name = "enum_field", type = DataType.CHARARRAY, converterClass = StringFromEnumConverter.class)
        private TestType enumField;

        @PigField(name = "property_string_field", type = DataType.CHARARRAY, method = "getPropertyStringField")
        private String propertyStringField;

        public PigFieldAnnotatedStub(String stringField,
                                     Integer integerField,
                                     Boolean booleanField,
                                     EmbeddedPigFieldAnnotatedStub embeddedPigFieldAnnotatedStub,
                                     TestType enumField) {

            this.stringField = stringField;
            this.integerField = integerField;
            this.booleanField = booleanField;
            this.embeddedPigFieldAnnotatedStub = embeddedPigFieldAnnotatedStub;
            this.enumField = enumField;
        }

        public String getPropertyStringField() {
            return "property string";
        }
    }

    private static final class EmbeddedPigFieldAnnotatedStub {

        @PigField(name = "embedded_string_field", type = DataType.CHARARRAY)
        private String embeddedStringField;

        private EmbeddedPigFieldAnnotatedStub(String embeddedStringField) {
            this.embeddedStringField = embeddedStringField;
        }
    }

    private static enum TestType {A, B}
}
