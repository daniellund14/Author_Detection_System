package online;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pkg.MRJob;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by DanielLund on 3/20/17.
 * Colorado State University
 * CS435
 */
public class AAVMapper extends Mapper<LongWritable, Text, Text, Text>{
    private static ArrayList<String> authors;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        authors = MRJob.readAuthors(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\\s+");
        if(line.length == 2){
            //Online TFIDF
            for(String author: authors){
                context.write(new Text(author + " " + line[0]), new Text("#"+ line[1]));
            }
        }else if(line.length == 3){
            //Offline TFIDF
            context.write(new Text(line[0] + " " + line[1]), new Text(line[2]));
        }
    }
}
