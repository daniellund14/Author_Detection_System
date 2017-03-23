package online;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by DanielLund on 3/17/17.
 * Colorado State University
 * CS435
 */
public class CosSimReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String[]> authors = new ArrayList<>();
        Double unknownTFIDF = 0.0;
        for(Text val: values){
            String[] pair = val.toString().split(",");
            if(pair[0].equals("xyz")){
                unknownTFIDF = new Double(pair[1]);
            }else{
                authors.add(pair);
            }
        }
        for(String[] pair: authors){
            String author = pair[0];
            Double b = new Double(pair[1]);
            Double unknownTimesB = unknownTFIDF * b;
            Double unknownSquare = unknownTFIDF * unknownTFIDF;
            Double bSquare = b * b;
            context.write(new Text(author + ",xyz"),  new Text(unknownTimesB.toString() + ","  + unknownSquare.toString() + "," + bSquare.toString()));
        }
    }
}
