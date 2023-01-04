package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MainJobs {
    private final static double N = 10000.0;

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
        /*
        Job CombinedWordCount = combinedWordCount(args[1] + "/part-r-00000", "Output3");
        if (!CombinedWordCount.waitForCompletion(true)) {
            System.exit(1);
        }*/
        Job Tr = TrJob(args[1] + "/part-r-00000", "Output3");
        if (!Tr.waitForCompletion(true)) {
            System.exit(1);
        }
        Job jobJoinWordCountNr = joinWordCountNr("Output2/part-r-00000", args[1] + "/part-r-00000", "Output4");
        if (!jobJoinWordCountNr.waitForCompletion(true)) {
            System.exit(1);
        }

        Job joinStep2 = joinWordCountNrStep2("Output4/part-r-00000", "Output5");
        System.exit(joinStep2.waitForCompletion(true) ? 0 : 1);

    }

    private static Job wordCountJob(String inputPath, String outputPath, String stopWordsPath) throws IOException, URISyntaxException {
        return JobBuilder.builder()
                .jarByClass(WordCount.class)
                .jobName("word count")
                .mapperClass(WordCount.MapperClass.class)
                .partitionerClass(WordCount.PartitionerClass.class)
                //.combinerClass(WordCount.CombinerClass.class)
                //.reducerClass(WordCount.ReducerClass.class)
                .reducerClass(WordCount.CombinerClass.class) //FIXME!
                .inputPath(inputPath)
                .outputPath(outputPath)
                .cacheFile(new URI(stopWordsPath))
                .mapOutputValueClass(LongWritable.class)
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
                .mapOutputValueClass(LongWritable.class)
                .outputValueClass(LongWritable.class)
                .inputPath(inputPath)
                .outputPath(outputPath)
                .build();
    }

    private static Job TrJob(String inputPath, String outputPath) throws IOException {
        return JobBuilder.builder()
                .jarByClass(Tr.class)
                .jobName("Tr")
                .mapperClass(Tr.MapperClass.class)
                .partitionerClass(Tr.PartitionerClass.class)
                .reducerClass(Tr.ReducerClass.class)
                .combinerClass(Tr.ReducerClass.class)
                .mapOutputValueClass(LongWritable.class)
                .outputValueClass(LongWritable.class)
                .inputPath(inputPath)
                .outputPath(outputPath)
                .build();
    }

    private static Job joinWordCountNr(String NrInputPath, String wordCountInputPath, String outputPath) throws IOException {
        return JobBuilder.builder()
                .jarByClass(JoinWordCountNr.class)
                .jobName("Join - {word count, Nr} ")
                .partitionerClass(JoinWordCountNr.PartitionerClass.class)
                .reducerClass(JoinWordCountNr.ReducerClass.class)
                .addInputPath(NrInputPath, JoinWordCountNr.MapperClassNr.class)
                .addInputPath(wordCountInputPath, JoinWordCountNr.MapperClassWordCount.class)
                .outputPath(outputPath)
                .setVariable(N)
                .build();
    }

    private static Job joinWordCountNrStep2(String joinWordCountNrInputPath, String outputPath) throws IOException {
        return JobBuilder.builder()
                .jarByClass(JoinWordCountNrStep2.class)
                .jobName("Join-2 - {word count, Nr} ")
                .mapperClass(JoinWordCountNrStep2.MapperClass.class)
                .partitionerClass(JoinWordCountNrStep2.PartitionerClass.class)
                .reducerClass(JoinWordCountNrStep2.ReducerClass.class)
                .inputPath(joinWordCountNrInputPath)
                .outputPath(outputPath)
                .build();
    }


}
