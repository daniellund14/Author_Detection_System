import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by DanielLund on 3/1/17.
 */
public class IDFReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Integer count = 0;
        String value = "";
        Long number_authors = context.getConfiguration().getLong("NUMBER_AUTHORS", -1);
        for(Text val: values) {
            count++;
        }
        Double IDF = Math.log10(number_authors/(new Double(count)));

        //context.write(key, new Text(IDF.toString() + "\t" + value));
        context.write(key, new Text(IDF.toString()));


    }
}
