package org.example;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairOutputFormat extends FileOutputFormat<Text, PairWritable> {

    @Override
    public RecordWriter<Text, PairWritable> getRecordWriter(TaskAttemptContext job) throws IOException {
        Path outputPath = getOutputPath(job);
        FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
        return new PairRecordWriter(fs.create(new Path(outputPath, "output.txt")));
    }

    private static class PairRecordWriter extends RecordWriter<Text, PairWritable> {

        private final DataOutputStream out;

        public PairRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(Text key, PairWritable value) throws IOException {
            // Write the key and value using the toString method
            out.writeBytes(key.toString() + "\t" + value.toString() + "\n");
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }
}