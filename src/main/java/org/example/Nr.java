package org.example;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.TextOutputs.CombinedWordsOutput;
import org.example.TextOutputs.WordCountOutput;


public class Nr {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1L);
        private final String N = "0"; //because we are sorting by the key the first line is going to be the number of rows. And 0 is not possible.

        //<ngram> <count1> <count2>
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            CombinedWordsOutput word = new CombinedWordsOutput(value);
            //emit <part, count>, <one>
            context.write(new Text("0\t" + word.getCount0()), one); //calc Nr
            context.write(new Text("1\t" + word.getCount1()), one); //calc Nr

            //emit <N>, <one> where N = 0.
            //context.write(new Text(N), new LongWritable(word.getCount0() + word.getCount1())); //calc N
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