package offline;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by DanielLund on 3/14/17.
 * Colorado State University
 * CS435
 */
public class AAVReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        for(Text val: values){
            String term = val.toString().split(":")[0];
            context.write(new Text(key), new Text(val.toString()));
        }

    }
}
