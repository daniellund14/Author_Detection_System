package online;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by lunddan on 3/23/17.
 */
public class CosSimCombiner extends Reducer<Text, Text, Text, Text>{
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
        String line = sumAB + "," + sumASquare + "," + sumBSquare;
        context.write(key, new Text(line));
    }
}
