import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by DanielLund on 2/27/17.
 */
public class TermFrequencyReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Collection<Text> values, Context context) throws
            IOException, InterruptedException {
        Double max = -1.0;
        List<Text> cache = new ArrayList<>();
        for (Text s: values){
            String[] split = s.toString().split(" ");
            Double frequency = new Double(split[1]);
            if(frequency > max){
                max = frequency;
            }
            cache.add(s);
        }

            context.write(new Text(max.toString()), new Text());

    }
}