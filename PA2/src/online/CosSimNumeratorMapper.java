package online;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by DanielLund on 3/21/17.
 * Colorado State University
 * CS435
 */
public class CosSimNumeratorMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input format is <Author Name> <Term> <Offline TFIDF> <Online TFIDF>
        String[] line = value.toString().split("\\s+");
        String author = line[0];
        String term = line[1];
        String offlineTFIDF = line[2];
        String onlineTFIDF = line[3];
        context.write(new Text(author), new Text(offlineTFIDF + " " + onlineTFIDF));
    }
}
