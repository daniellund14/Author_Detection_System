import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by DanielLund on 3/6/17.
 */
public class CountAuthorsMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Mapper.Context context) throws
            IOException, InterruptedException {
        String[] termFrequency = value.toString().split("\\s+");
        String author = termFrequency[0];
        context.write(new Text("one"), new Text(author));


    }
}
