package com.intentmedia.pig;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

@SuppressWarnings("rawtypes")
public final class StubRecordReader extends RecordReader {

    private final Text stubbedCurrentValue;

    StubRecordReader(Text stubbedCurrentValue) {
        this.stubbedCurrentValue = stubbedCurrentValue;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return true;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return stubbedCurrentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }
}
