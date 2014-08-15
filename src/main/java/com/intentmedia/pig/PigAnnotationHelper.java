package com.intentmedia.pig;

import com.intentmedia.convert.ToPigTypeConverter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.jetbrains.annotations.Nullable;

import javax.persistence.Embedded;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PigAnnotationHelper {

    private final List<Field> pigFields;
    private final List<Field> embeddedFields;
    private final List<Field> superClassPigFields;

    private final Map<Field, PigAnnotationHelper> annotationHelpers = new HashMap<Field, PigAnnotationHelper>();
    private final List<ResourceSchema.ResourceFieldSchema> resourceFieldSchemas =
            new ArrayList<ResourceSchema.ResourceFieldSchema>();

    public PigAnnotationHelper(Class<?> clazz) {
        pigFields = getAnnotatedFields(clazz, PigField.class);
        embeddedFields = getAnnotatedFieldsIncludingInherited(clazz, Embedded.class);
        superClassPigFields = getAnnotatedFieldsIncludingInherited(clazz.getSuperclass(), PigField.class);

        appendMetaData(pigFields);

        for (Field embeddedField : embeddedFields) {
            PigAnnotationHelper embeddedFieldAnnotationHelper = new PigAnnotationHelper(embeddedField.getType());
            annotationHelpers.put(embeddedField, embeddedFieldAnnotationHelper);
            resourceFieldSchemas.addAll(embeddedFieldAnnotationHelper.resourceFieldSchemas);
        }
        appendMetaData(superClassPigFields);
    }

    public ResourceSchema getResourceSchema() {
        return new ResourceSchema()
                .setFields(resourceFieldSchemas.toArray(new ResourceSchema.ResourceFieldSchema[resourceFieldSchemas.size()]));
    }

    @SuppressWarnings("rawtypes,unchecked")
    public <T> Tuple toTuple(T instance, TupleFactory tupleFactory)
            throws IllegalAccessException, IOException,
            NoSuchMethodException, InvocationTargetException, InstantiationException {

        Tuple tuple = tupleFactory.newTuple(getResourceSchema().getFields().length);
        List<Object> tupleValues = getTupleValues(instance);

        for (int i = 0; i < tupleValues.size(); i++) {
            tuple.set(i, tupleValues.get(i));
        }

        return tuple;
    }

    private void appendMetaData(List<Field> fields) {
        for (Field pigAnnotatedField : fields) {
            PigField pigField = pigAnnotatedField.getAnnotation(PigField.class);
            ResourceSchema.ResourceFieldSchema resourceFieldSchema = new ResourceSchema.ResourceFieldSchema();
            String fieldName = pigField.name();
            resourceFieldSchema.setName(fieldName);
            resourceFieldSchema.setType(pigField.type());
            resourceFieldSchemas.add(resourceFieldSchema);
        }
    }

    private <T> List<Object> getTupleValues(@Nullable T instance)
            throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, InstantiationException, IOException {

        List<Object> tupleValues = new ArrayList<Object>(resourceFieldSchemas.size());

        if (null != instance) {

            addTupleValues(instance, pigFields, tupleValues);

            for (Field field : embeddedFields) {
                PigAnnotationHelper embeddedFieldAnnotationHelper = annotationHelpers.get(field);

                // Note that embedded objects do not support method access
                tupleValues.addAll(embeddedFieldAnnotationHelper.getTupleValues(field.get(instance)));
            }

            addTupleValues(instance, superClassPigFields, tupleValues);
        }
        else {
            for (Field directPigField : pigFields) {
                tupleValues.add(null);
            }

            for (Field field : embeddedFields) {
                PigAnnotationHelper embeddedFieldAnnotationHelper = annotationHelpers.get(field);
                tupleValues.addAll(embeddedFieldAnnotationHelper.getTupleValues(null));
            }
        }

        return tupleValues;
    }

    @SuppressWarnings("rawtypes,unchecked")
    private <T> void addTupleValues(T instance, List<Field> fields, List<Object> tupleValues)
            throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException, IOException {

        for (Field field : fields) {
            PigField pigField = field.getAnnotation(PigField.class);
            String methodName = pigField.method();
            Object value;

            if (methodName.isEmpty()) {
                value = field.get(instance);
            }
            else {
                value = instance.getClass().getMethod(methodName).invoke(instance);
            }

            if (value != null) {
                Class<? extends ToPigTypeConverter> converterClass = pigField.converterClass();
                ToPigTypeConverter converter = converterClass.getConstructor().newInstance();
                tupleValues.add(converter.convert(value));
            }
            else {
                tupleValues.add(null);
            }
        }
    }

    private static <T> List<Field> getAnnotatedFields(Class<T> clazz, Class<? extends Annotation> annotationClazz) {
        Field[] declaredFields = clazz.getDeclaredFields();
        List<Field> fieldsWithAnnotations = new ArrayList<Field>();

        for (Field field : declaredFields) {
            if (field.isAnnotationPresent(annotationClazz)) {
                field.setAccessible(true);
                fieldsWithAnnotations.add(field);
            }
        }
        return fieldsWithAnnotations;
    }

    public static List<Field> getAnnotatedFieldsIncludingInherited(final Class<?> clazz, final Class<? extends Annotation> pigFieldClass) {
        List<Field> annotatedFields = new ArrayList<Field>();
        Class<?> current = clazz;
        while (current != Object.class) {
            annotatedFields.addAll(getAnnotatedFields(current, pigFieldClass));
            current = current.getSuperclass();
        }
        return annotatedFields;
    }
}
