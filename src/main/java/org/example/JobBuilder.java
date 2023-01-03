package org.example;

import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.net.URI;

public class JobBuilder{
    private static boolean combinerGotSetFlag;
    private static Job job;

    private JobBuilder() {

    }

    public static JobBuilder builder() throws IOException {
        combinerGotSetFlag = false;
        Configuration configuration = new Configuration();

        job = Job.getInstance(configuration);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
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
        if(combinerGotSetFlag)
            return this;
        job.setCombinerClass(reducerClass);
        return this;
    }

    public JobBuilder combinerClass(Class<? extends Reducer> reducerClass) {
        job.setCombinerClass(reducerClass);
        combinerGotSetFlag = true;
        return this;
    }

    public JobBuilder partitionerClass(Class<? extends Partitioner> partitionerClass) {
        job.setPartitionerClass(partitionerClass);
        return this;
    }

    public JobBuilder mapOutputKeyClass(Class<? extends Writeable> mapOutputKeyClass) {
        job.setMapOutputKeyClass(mapOutputKeyClass);
        return this;
    }

    public JobBuilder cacheFile(URI file){
        job.addCacheFile(file);
        return this;
    }
}
