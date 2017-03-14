import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by DanielLund on 3/11/17.
 */
public class AuthorAAVReducer  extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        String val = "";
        for(Text s: values){
            if(values.iterator().hasNext()){
                val += s.toString() + " ";
            }else{
                val += s.toString();
            }
        }
        String[] vals = val.split("\\s+");
        Double TF = 0.0;
        Double IDF = 0.0;

        if(vals.length == 2){
            if(vals[0].indexOf("#") == -1){
                vals[1] = vals[1].replace("#","");
                IDF = new Double(vals[1]);
                TF = new Double(vals[0]);
            }else{
                vals[0] = vals[0].replace("#", "");
                IDF = new Double(vals[0]);
                TF = new Double(vals[1]);
            }
        }else if(vals.length == 1){
            vals[0] = vals[0].replace("#", "");
            IDF = new Double(vals[0]);
            TF = .5;
        }
        Double TFxIDF = TF * IDF;
        context.write(key, new Text(TFxIDF.toString()));
    }

}
