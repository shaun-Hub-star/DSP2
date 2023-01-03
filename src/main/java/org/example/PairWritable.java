package org.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

// Custom writable class that can hold a pair of LongWritable objects
class PairWritable implements Writable  {

    private LongWritable first;
    private LongWritable second;

    //public PairWritable() {}

    public PairWritable(LongWritable first, LongWritable second) {
        this.first = first;
        this.second = second;
    }

    public PairWritable(long first, long second) {
        this.first = new LongWritable(first);
        this.second = new LongWritable(second);
    }

    public LongWritable getFirst() {
        return first;
    }

    public void setFirst(LongWritable first) {
        this.first = first;
    }

    public LongWritable getSecond() {
        return second;
    }

    public void setSecond(LongWritable second) {
        this.second = second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Serialize the pair objects
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // Deserialize the pair objects
        first = new LongWritable();
        first.readFields(in);
        second = new LongWritable();
        second.readFields(in);
    }

    @Override
    public String toString() {
        return first.toString() + "\t" + second.toString();
    }

}