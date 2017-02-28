import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>{
	static final String START  = "_START_";
	static final String END  = "_END_";

	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException{
		String[] authorDateWords = value.toString().split("<===>");
		String[] authorArray = authorDateWords[0].split(" ");
		String author = authorArray[authorArray.length-1];
		String[] dateArray = authorDateWords[1].split(" ");
		String year = dateArray[dateArray.length-1];
		String sentence = authorDateWords[2];
		sentence = sentence.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();
		String[] words = sentence.split("\\s+");
		String previous = START;
		for(String word: words) {
			if(word.equals(word.length() == 0))
				continue;
			context.write(new Text(word + "\t" + year), new Text("1A"));
			context.write(new Text(word + "\t" + author), new Text("1B"));
			context.write(new Text(previous + " " + word + "\t" + year), new Text("2A"));
			context.write(new Text(previous + " " + word + "\t" + author), new Text("2B"));
			previous = word;
		}
		context.write(new Text(previous + " " + END + "\t" + year), new Text("2A"));
		context.write(new Text(previous + " " + END + "\t" + author), new Text("2B"));

	}

}