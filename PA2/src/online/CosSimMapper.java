package online;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by DanielLund on 3/16/17.
 * Colorado State University
 * CS435
 */
public class CosSimMapper extends Mapper<LongWritable, Text, Text, Text> {

    ArrayList<String> authors;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        authors = pkg.MRJob.readAuthors(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\\s+");
        if(line.length == 3){
            //Offline file
            String author = line[0];
            String term_tfidf = line[1] + ":" + line[2];
            context.write(new Text("xyz " + author), new Text(term_tfidf));
        }else if(line.length == 4){
            //Online file
            String term_tfidf = line[2] + ":" + line[3];
            for(String author: authors){
                context.write(new Text("xyz " + author), new Text(term_tfidf));
            }
        }else{
            System.err.println("Invalid input file");
        }
    }
}
