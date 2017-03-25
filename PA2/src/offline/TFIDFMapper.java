package offline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pkg.AuthorCosSim;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by DanielLund on 3/22/17.
 * Colorado State University
 * CS435
 */
public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
    static ArrayList<String> authors;
    static ArrayList<AuthorCosSim> idf;

    public void map(LongWritable key, Text value, Mapper.Context context) throws
            IOException, InterruptedException {
        String[] line = value.toString().split("\\s+");
        if(line.length == 2) {
            // IDF File
            String term = line[0];
            context.write(new Text(term), new Text("#"+line[1]));
        }else if(line.length == 3){
            // TF File
            String author = line[0];
            String term = line[1];
            String tf = line[2];
            context.write(new Text(term), new Text(author + "," + tf));
        }else{
            System.err.println("Incorrect input file");
        }

    }
}
