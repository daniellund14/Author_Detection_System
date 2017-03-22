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
public class RootSumMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\\s+");
        if(line.length == 2){
            String tfidf = line[1];
            context.write(new Text("<xyz>"), new Text(tfidf));
        }else if(line.length == 3){
            String author = line[0];
            String tfidf = line[2];
            context.write(new Text(author), new Text(tfidf));
        }

    }
}
