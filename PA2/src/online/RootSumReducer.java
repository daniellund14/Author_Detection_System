package online;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by DanielLund on 3/21/17.
 * Colorado State University
 * CS435
 */
public class RootSumReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        for(Text val: values){
            Double tfidf = new Double(val.toString());
            sum +=  (tfidf * tfidf);
        }
        Double rootTFIDF = Math.sqrt(sum);
        context.write(key, new Text(rootTFIDF.toString()));
    }
}
