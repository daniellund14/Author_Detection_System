import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by DanielLund on 3/31/17.
 * Colorado State University
 * CS435
 */
public class WikiRecordReader extends RecordReader {
    Path file = null;
    Configuration conf = null;
    LongWritable key = new LongWritable();
    Text value = new Text();
    private FSDataInputStream fsDataInputStream;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private long start;
    private long end;
    private byte[] startTags;
    private byte[] endTags;
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        start = split.getStart();
        end = start + split.getLength();
        file = split.getPath();
        conf = taskAttemptContext.getConfiguration();
        FileSystem fs = file.getFileSystem(conf);
        startTags = "REVISION".getBytes();
        endTags = "\n\n".getBytes();
        fsDataInputStream = fs.open(file);
        fsDataInputStream.seek(start);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        return false;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {

        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
