package online;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by DanielLund on 3/20/17.
 * Colorado State University
 * CS435
 */
public class AAVReducer extends Reducer<Text, Text, Text, Text>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String online = "";
        String offline = "";
        for(Text val: values){
            if(val.toString().contains("#")){
                online = val.toString();
            }else{
                offline = val.toString();
            }
        }
        context.write(key, new Text(offline + " " + online));
    }
}

