package org.example;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.TextOutputs.WordCountOutput;


public class Nr {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1L);
        private final String N = "0"; //because we are sorting by the key the first line is going to be the number of rows. And 0 is not possible.
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            WordCountOutput word = new WordCountOutput(value);
            context.write(new Text("" + word.getCount()), one); //calc Nr //FIXME???? changed. was context.write(word.getText(), one)
            context.write(new Text(N), new LongWritable(word.getCount())); //calc N
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            //System.out.println("The sum is:" + sum);
            context.write(key, new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}