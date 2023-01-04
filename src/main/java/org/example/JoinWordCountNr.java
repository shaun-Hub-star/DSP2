package org.example;

import java.io.IOException;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.example.TextOutputs.CombinedWordsOutput;
import org.example.TextOutputs.NrOutput;
import org.example.TextOutputs.WordCountOutput;


public class JoinWordCountNr {
    public static class MapperClassWordCount extends Mapper<LongWritable, Text, Text, Text> {

        //<ngram> <count1> <count2>
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            CombinedWordsOutput word = new CombinedWordsOutput(value);
            //emit <r0>, <W \t words>
            //emit <r1>, <W \t words>
            context.write(new Text("0\t" + word.getCount0()), new Text("W\t" + word.getWords())); //calc Nr0
            context.write(new Text("1\t" + word.getCount1()), new Text("W\t" + word.getWords())); //calc Nr1
        }

    }
    public static class MapperClassNr extends Mapper<LongWritable, Text, Text, Text> {
        //private final String N = "0"; //because we are sorting by the key the first line is going to be the number of rows. And 0 is not possible.

        //<0/1> <r> <Nr^0/1>
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            NrOutput word = new NrOutput(value);
            long r = word.getR();
            long Nr = word.getNr();//nr
            context.write(new Text(word.getPart() + "\t" + r), new Text("Nr\t" + Nr)); //calc N

        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //key: <part>\t<r>
            //value: W\t<words> OR Nr\t<Nr>
            System.out.println("[DEBUG] combiner received key: " + key);
            long nr = 0;
            long nrCount = 0;
            List<String> wordCountRecords = new LinkedList<>();
            for (Text value : values) {
                String[] parts = value.toString().split("\\t");
                if (parts[0].equals("W")) {
                    //input from wordCount
                    wordCountRecords.add(parts[1]);
                    System.out.println("[DEBUG] combiner got the word: " + parts[1] + " for key: " + key);

                } else {
                    //input from Nr. should only be one!
                    nrCount++;
                    nr += Long.parseLong(parts[1]);
                    System.out.println("[DEBUG] combiner got the nr: " + nr + " for key: " + key);
                }
            }
            System.out.println("[DEBUG] NR count is :" + nrCount);

            //emit <words>\t<Nr>
            for (String w : wordCountRecords) {
                System.out.println("[DEBUG] combiner emitting: " + w + "\t" + nr);
                context.write(new Text(w), new Text(String.valueOf(nr)));
            }

        }

    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}