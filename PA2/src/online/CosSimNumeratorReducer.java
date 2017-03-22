package online;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by DanielLund on 3/21/17.
 * Colorado State University
 * CS435
 */
public class CosSimNumeratorReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        for(Text val: values){
            String[] value = val.toString().split("\\s+");
            Double offline = new Double(value[0]);
            Double online = new Double(value[1].replace("#",""));
            sum += offline * online;
        }
        context.write(key, new Text(sum.toString()));
    }
}
