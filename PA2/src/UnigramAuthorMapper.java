import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by DanielLund on 2/23/17.
 */
public class UnigramAuthorMapper extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, Mapper.Context context) throws
            IOException, InterruptedException{
        String[] authorDateWords = value.toString().split("<===>");
        String[] authorArray = authorDateWords[0].split(" ");
        String author = authorArray[authorArray.length-1];
        String[] dateArray = authorDateWords[1].split(" ");
        String year = dateArray[dateArray.length-1];
        String sentence = authorDateWords[2];
        sentence = sentence.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();
        String[] words = sentence.split("\\s+");
        for(String word: words) {
            if(word.length() == 0)
                continue;
            context.write(new Text(author + "\t" + word), new Text("one"));
        }

    }
}
