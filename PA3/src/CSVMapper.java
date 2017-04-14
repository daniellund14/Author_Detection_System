import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by DanielLund on 4/13/17.
 * Colorado State University
 * CS435
 */
public class CSVMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\n");
        String[] revision = lines[0].split("\\s+");
        String keyOut = "";
        for(int i = 1; i < revision.length; i++){
            if(i == revision.length - 1)
                keyOut += revision[i];
            else if (i == 2){
                keyOut = revision[i] + "," + keyOut;
            }
            else
                keyOut += revision[i] + ",";
        }
        context.write(new Text(keyOut), new Text());
    }
}
