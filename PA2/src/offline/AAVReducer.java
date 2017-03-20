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
        String AAV = "";
        for (Text val: values){
            if(values.iterator().hasNext()){
                AAV += val.toString() + ",";
            }else{
                AAV += val.toString();
            }
        }

        context.write(key, new Text(AAV));

    }
}
