package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


import java.net.URI;
import java.util.HashSet;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.example.TextOutputs.PartedText;


public class WordCount {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1L);
        //private final static Text word = new Text();
        HashSet<String> stopWords;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            stopWords = new HashSet<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(context.getCacheFiles()[0])))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.toLowerCase());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //jar input[s3://ngrams/eng-all/data] output

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            PartedText word = new PartedText(key, value);
            if (word.getNumOfWords() != 3 || word.hasStopWord(stopWords) || word.hasIllegalCharacter())
                return;

            context.write(word.getText(), one);

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