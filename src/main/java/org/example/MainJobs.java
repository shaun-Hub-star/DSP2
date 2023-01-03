package org.example;

import jdk.jfr.internal.tool.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MainJobs {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Job wordCountJob = wordCountJob(args[0], args[1], args[2]);


        //job.setInputFormatClass(SequenceFileInputFormat.class);
        //FileInputFormat.setInputDirRecursive(job, true);
        //System.exit(job1.waitForCompletion(true) ? 0 : 1);
        if (!wordCountJob.waitForCompletion(true)) {
            System.exit(1);
        }
        Job Nr = NrJob(args[1] + "/part-r-00000", "Output2");
        if (!Nr.waitForCompletion(true)) {
            System.exit(1);
        }

    }

    private static Job wordCountJob(String inputPath, String outputPath, String stopWordsPath) throws IOException, URISyntaxException {
        return JobBuilder.builder()
                .jarByClass(WordCount.class)
                .jobName("word count")
                .mapperClass(WordCount.MapperClass.class)
                .partitionerClass(WordCount.PartitionerClass.class)
                .reducerClass(WordCount.ReducerClass.class)
                .inputPath(inputPath)
                .outputPath(outputPath)
                .cacheFile(new URI(stopWordsPath))
                .build();
    }

    private static Job NrJob(String inputPath, String outputPath) throws IOException {
        return JobBuilder.builder()
                .jarByClass(Nr.class) //TODO!!!!!!!!!
                .jobName("Nr")
                .mapperClass(Nr.MapperClass.class)
                .partitionerClass(Nr.PartitionerClass.class)
                .reducerClass(Nr.ReducerClass.class)
                .inputPath(inputPath)
                .outputPath(outputPath)
                .build();
    }



}
