package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.net.URI;

public class JobBuilder{
    private static Job job;
    private static Configuration configuration;
    private JobBuilder() {

    }

    public static JobBuilder builder() throws IOException {
        configuration = new Configuration();

        job = Job.getInstance(configuration);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return new JobBuilder();
    }
    public Job build() {
        return job;
    }
    public JobBuilder jarByClass(Class<?> jarClass){
        job.setJarByClass(jarClass);
        return this;
    }

    public JobBuilder jobName(String jobName){
        job.setJobName(jobName);
        return this;
    }

    public JobBuilder inputPath(String inputPath) throws IOException {
        FileInputFormat.addInputPath(job, new Path(inputPath));
        return this;
    }

    public JobBuilder outputPath(String outputPath) {
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return this;
    }

    public JobBuilder mapperClass(Class<? extends Mapper> mapperClass) {
        job.setMapperClass(mapperClass);
        return this;
    }

    public JobBuilder reducerClass(Class<? extends Reducer> reducerClass) {
        job.setReducerClass(reducerClass);
        return this;
    }

    public JobBuilder combinerClass(Class<? extends Reducer> reducerClass) {
        job.setCombinerClass(reducerClass);
        return this;
    }

    public JobBuilder partitionerClass(Class<? extends Partitioner> partitionerClass) {
        job.setPartitionerClass(partitionerClass);
        return this;
    }

    public JobBuilder cacheFile(URI file){
        job.addCacheFile(file);
        return this;
    }

    public JobBuilder mapOutputKeyClass(Class<?> mapOutputKeyClass) {
        job.setMapOutputKeyClass(mapOutputKeyClass);
        return this;
    }

    public JobBuilder mapOutputValueClass(Class<?> mapOutputValueClass){
        job.setMapOutputValueClass(mapOutputValueClass);
        return this;
    }

    public JobBuilder outputKeyClass(Class<?> outputKeyClass){
        job.setOutputKeyClass(outputKeyClass);
        return this;
    }

    public JobBuilder outputValueClass(Class<?> outputValueClass){
        job.setOutputValueClass(outputValueClass);
        return this;
    }

    public JobBuilder outputFormatClass(Class<? extends OutputFormat> outputFormat){
        job.setOutputFormatClass(outputFormat);
        return this;
    }

    public JobBuilder addInputPath(String inputPath, Class<? extends Mapper> mapperClass){
        MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class, mapperClass);
        return this;
    }

    public JobBuilder setVariable(double N){
        configuration.setDouble("N", N);
        return this;
    }

}
