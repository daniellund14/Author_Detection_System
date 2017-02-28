import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
public class WordCountReducer extends Reducer<Text,Text,Text,IntWritable>{
	private MultipleOutputs<Text, Text> mos;
	public void setup(Context context){
		mos = new MultipleOutputs(context);
	}
	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {
		Integer count = 0;

		for(Text val: values) {
			count++;
		}

		context.write(key, new IntWritable(count));
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
	}

}