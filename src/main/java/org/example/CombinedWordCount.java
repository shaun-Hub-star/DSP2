package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.TextOutputs.WordCountOutput;

import java.io.IOException;

public class CombinedWordCount {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            WordCountOutput word = new WordCountOutput(value);
            //<3gram>\t(<part> <count>)
            context.write(word.getWords(), new Text(word.getPart() + " " + word.getCount()));

        }


    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum0 = 0;
            long sum1 = 0;

            for (Text value : values) {
                String[] parts = value.toString().split("\\s");
                int part = Integer.parseInt(parts[0]);
                long count = Integer.parseInt(parts[1]);
                if (part == 0)
                    sum0 += count;
                else
                    sum1 += count;
            }
            context.write(key, new Text("" + sum0 + "\t" + sum1));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
