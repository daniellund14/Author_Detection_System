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
        Text keyOut = new Text();
        Text valOut = new Text();
        for(String[] pair: authors){
            String author = pair[0];
            Double b = new Double(pair[1]);
            Double AB = unknownTFIDF * b;
            Double aSquare = unknownTFIDF * unknownTFIDF;
            Double bSquare = b * b;
            keyOut.set(author + ",xyz");
            valOut.set(AB.toString() + ","  + aSquare.toString() + "," + bSquare.toString());
            //valOut.set("Unkown TFIDF:"+unknownTFIDF.toString() + " B value:" + b.toString() +
            //" Unknown TFIDF * B:" + AB.toString() + " Unknown Squared:" + aSquare.toString() + " B value Sqaured:" + bSquare.toString());
            context.write(keyOut, valOut);
        }
    }
}
