package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
        Job CombinedWordCount = combinedWordCount(args[1] + "/part-r-00000", "Output3");
        if (!CombinedWordCount.waitForCompletion(true)) {
            System.exit(1);
        }
        Job Tr = TrJob("Output3/part-r-00000", "Output4");
        System.exit(Tr.waitForCompletion(true) ? 0 : 1);

    }

    private static Job wordCountJob(String inputPath, String outputPath, String stopWordsPath) throws IOException, URISyntaxException {
        return JobBuilder.builder()
                .jarByClass(WordCount.class)
                .jobName("word count")
                .mapperClass(WordCount.MapperClass.class)
                .partitionerClass(WordCount.PartitionerClass.class)
                .reducerClass(WordCount.ReducerClass.class)
                .combinerClass(WordCount.ReducerClass.class)
                .inputPath(inputPath)
                .outputPath(outputPath)
                .cacheFile(new URI(stopWordsPath))
                .build();
    }

    private static Job NrJob(String inputPath, String outputPath) throws IOException {
        return JobBuilder.builder()
                .jarByClass(Nr.class)
                .jobName("Nr")
                .mapperClass(Nr.MapperClass.class)
                .partitionerClass(Nr.PartitionerClass.class)
                .reducerClass(Nr.ReducerClass.class)
                .combinerClass(Nr.ReducerClass.class)
                .inputPath(inputPath)
                .outputPath(outputPath)
                .build();
    }

    private static Job combinedWordCount(String inputPath, String outputPath) throws IOException {
        return JobBuilder.builder()
                .jarByClass(CombinedWordCount.class) //TODO!!!!!!!!!
                .jobName("Combine words ")
                .mapperClass(CombinedWordCount.MapperClass.class)
                .partitionerClass(CombinedWordCount.PartitionerClass.class)
                .reducerClass(CombinedWordCount.ReducerClass.class)
                .mapOutputValueClass(Text.class)
                .outputValueClass(Text.class)
                .inputPath(inputPath)
                .outputPath(outputPath)
                .setOutputFormatClass(TextOutputFormat.class)
                .build();
    }
    private static Job TrJob(String inputPath, String outputPath) throws IOException {
        return JobBuilder.builder()
                .jarByClass(Tr.class)
                .jobName("Nr")
                .mapperClass(Tr.MapperClass.class)
                .partitionerClass(Tr.PartitionerClass.class)
                .reducerClass(Tr.ReducerClass.class)
                .combinerClass(Tr.ReducerClass.class)
                .inputPath(inputPath)
                .outputPath(outputPath)
                .build();
    }


}
