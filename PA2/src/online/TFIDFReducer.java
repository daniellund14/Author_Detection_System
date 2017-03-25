package online;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by DanielLund on 3/15/17.
 * Colorado State University
 * CS435
 */
public class TFIDFReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Double idf = null;
        Double tf = 0.5;
        for(Text val: values){
            if(val.toString().contains("#")){
                idf = new Double(val.toString().replace("#", ""));
            }else{
                tf = new Double(val.toString());
            }
        }
        if(idf != null){
            Double tfidf = tf * idf;
            context.write(new Text("xyz" + " "  + key), new Text(tfidf.toString()));
        }
    }
}


