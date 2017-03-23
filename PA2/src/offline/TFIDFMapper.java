package offline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pkg.IdfTerm;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by DanielLund on 3/11/17.
 */
public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
    static ArrayList<String> authors;
    static ArrayList<IdfTerm> idf;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        authors = pkg.MRJob.readAuthors(context.getConfiguration());
    }

    public void map(LongWritable key, Text value, Mapper.Context context) throws
            IOException, InterruptedException {
        String[] line = value.toString().split("\\s+");
        if(line.length == 2) {
            // IDF File
            String term = line[0];
            Text termText = new Text("#" + line[0]);
            Text idf = new Text("#" + line[1]);
            for (String author : authors) {
                context.write(new Text(author + " " + term), idf);
            }
        }else if(line.length == 3){
            // TF File
            String author = line[0];
            String term = line[1];
            String tf = line[2];
            context.write(new Text(author + " " + term), new Text(tf));
        }else{
            System.err.println("Incorrect input file");
        }

    }
}
