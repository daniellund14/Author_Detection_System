import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by DanielLund on 3/1/17.
 */
public class IDFMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Mapper.Context context) throws
            IOException, InterruptedException {
        String[] termFrequency = value.toString().split("\\s+");
        String author = termFrequency[0];
        String term = termFrequency[1];
        String tf = termFrequency[2];
        context.write(new Text(term), new Text(author));

    }

}
