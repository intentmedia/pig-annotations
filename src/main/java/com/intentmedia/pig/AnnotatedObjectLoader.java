package com.intentmedia.pig;

import com.intentmedia.convert.RecordInflater;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("rawtypes,unchecked")
public class AnnotatedObjectLoader extends LoadFunc implements LoadMetadata {

    private RecordReader reader = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private PigAnnotationHelper pigAnnotationHelper;
    private RecordInflater<?> deserializer;

    public AnnotatedObjectLoader(String recordClassName)
            throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            InstantiationException {

        Class<?> clazz = Class.forName(recordClassName);
        if (clazz.isAnnotationPresent(PigLoadable.class)) {
            Class<? extends RecordInflater<?>> deserializer = clazz.getAnnotation(PigLoadable.class).recordInflater();
            this.deserializer = deserializer.getConstructor().newInstance();

            pigAnnotationHelper = new PigAnnotationHelper(clazz);
        }
        else {
            throw new RuntimeException(String
                    .format("[%s] does not support loading data for [%s]. [%s] must implement the [%s] interface and be annotated with [%s] annotations.",
                            AnnotatedObjectLoader.class.getSimpleName(),
                            clazz.getSimpleName(),
                            clazz.getSimpleName(),
                            PigLoadable.class.getSimpleName(),
                            PigField.class.getSimpleName()
                    ));
        }
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        Text value;
        try {
            if (!reader.nextKeyValue()) {
                return null;
            }

            value = (Text) reader.getCurrentValue();
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException(ie);
        }

        Object data = deserializer.convert(value);

        try {
            return pigAnnotationHelper.toTuple(data, tupleFactory);
        }
        catch (IllegalAccessException exception) {
            throw new RuntimeException(exception);
        }
        catch (NoSuchMethodException exception) {
            throw new RuntimeException(exception);
        }
        catch (InvocationTargetException exception) {
            throw new RuntimeException(exception);
        }
        catch (InstantiationException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        return pigAnnotationHelper.getResourceSchema();
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException {
    }
}
