package online;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by lunddan on 3/23/17.
 */
public class CosSimMapper2 extends Mapper<LongWritable, Text,Text, Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\\s+");
        context.write(new Text(line[0]), new Text(line[1]));
    }
}
