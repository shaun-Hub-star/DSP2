package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigInteger;

public class JoinWordCountNrStep2 {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        //private final String N = "0"; //because we are sorting by the key the first line is going to be the number of rows. And 0 is not possible.

        //<0/1> <r> <Nr^0/1>
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\t");
            context.write(new Text(parts[0]), new Text(parts[1])); //calc N

        }
    }



    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private static BigInteger N = new BigInteger("10000");//TODO!!!!!! remember to divide N by 2

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            BigInteger sum = new BigInteger("0");
            for(Text value: values){
                System.out.println("~~~~~~~value in Reduce class~~~~~~~~~ " + value);
                sum = sum.add(new BigInteger(value.toString()));
            }
            //emit <w>\t<N*(Nr^0 + Nr^1)>
            context.write(key, new Text(""+sum.multiply(N)));
        }
    }
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
