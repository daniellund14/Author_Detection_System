package online;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by DanielLund on 3/15/17.
 * Colorado State University
 * CS435
 */
public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text>{
    ArrayList<String> authors;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        authors = pkg.MRJob.readAuthors(context);
    }

    public void map(LongWritable key, Text value, Mapper.Context context) throws
            IOException, InterruptedException {
        String[] line = value.toString().split("\\s+" );
        if(line.length == 2){
            //IDF file
            String term = line[0];
            String idf = line[1];
            context.write(new Text(term), new Text("#"+idf));
        }else if(line.length == 3){
            //TF File
            String term = line [1];
            String tf = line[2];
            context.write(new Text(term), new Text(tf));
        }else {
            System.err.println("Incorrect input file format");
        }
    }
}

