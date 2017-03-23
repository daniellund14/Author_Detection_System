package offline;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by DanielLund on 3/11/17.
 * Colorado State University
 * CS435
 */
public class TFIDFReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Double IDF = 0.0;
        Double TF = 0.5;
        for(Text s: values){
            if(s.toString().contains("#")){
                IDF = new Double(s.toString().replace("#", ""));
            }else{
                TF = new Double(s.toString());
            }
        }
        Double TFxIDF = TF * IDF;
        if(TFxIDF != 0)
            context.write(key, new Text(TFxIDF.toString()));
    }

}
