import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


/**
 * Created by DanielLund on 3/6/17.
 */
public class CountAuthorsReducer extends Reducer<Text,Text,Text,Text>{
    public void reduce(Text key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        ArrayList<String> authors = new ArrayList<>();
        for(Text s: values){
            if(!authors.contains(s.toString())){
                authors.add(s.toString());
                context.write(new Text(s.toString()), new Text());
            }
        }

    }
}
