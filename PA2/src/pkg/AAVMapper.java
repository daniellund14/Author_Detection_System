package pkg;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by DanielLund on 3/14/17.
 * Colorado State University
 * CS435
 */
public class AAVMapper extends Mapper<LongWritable, Text, Text, Text> {

public void map(LongWritable key, Text value, Mapper.Context context) throws
        IOException, InterruptedException {
            String[] line = value.toString().split("\\s+");
            String author = line[0];
            String termTFIDF = line[1] + ":" + line[2];
            context.write(new Text(author), new Text(termTFIDF));
        }
}
