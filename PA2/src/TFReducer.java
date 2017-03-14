import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by DanielLund on 2/27/17.
 */
public class TFReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Double max = -1.0;
        Double Tf;
        String maxTerm = "";
        ArrayList<Text> cache = new ArrayList<>();

        for (Text s: values){
            Text copy = new Text(s);
            String[] split = s.toString().split(":");
            Double frequency = new Double(split[1]);
            if(frequency > max){
                max = frequency;
                maxTerm = split[0];
            }
            cache.add(copy);
        }
        Collections.sort(cache);
        for(Text s: cache){
            String[] split = s.toString().split(":");
            Double frequency = new Double(split[1]);
            Tf =.5 + .5 * (frequency/max);
            context.write(key, new Text(split[0] + " " + Tf.toString()));
        }
        //context.write(key, new Text(maxTerm + " " + max.toString()));


    }
}
