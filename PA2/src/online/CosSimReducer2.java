package online;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by lunddan on 3/23/17.
 */
public class CosSimReducer2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double sumAB = 0.0;
        Double sumASquare = 0.0;
        Double sumBSquare = 0.0;
        for(Text val: values){
            String[] numbers = val.toString().split(",");
            Double AB = new Double(numbers[0]);
            Double aSquare = new Double(numbers[1]);
            Double bSquare = new Double(numbers[2]);
            sumAB += AB;
            sumASquare += aSquare;
            sumBSquare += bSquare;
        }

        Double cosSim = ((sumAB)/(sumASquare * sumBSquare));
        String author = key.toString().split(",")[0];
        context.write(new Text(author), new Text(cosSim.toString()));
    }
}
