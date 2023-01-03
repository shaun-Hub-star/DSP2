package org.example;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.TextOutputs.CombinedWordsOutput;
import org.example.TextOutputs.WordCountOutput;


public class Tr {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            CombinedWordsOutput word = new CombinedWordsOutput(value);
            //emit <part0, count0>, <count1>
            if(word.getCount0() != 0)
                context.write(new Text( "01\t" + word.getCount0()), new LongWritable(word.getCount1()));

            //emit <part1, count1>, <count0>
            if(word.getCount1() != 0)
                context.write(new Text( "10\t" + word.getCount1()), new LongWritable(word.getCount0()));
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
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