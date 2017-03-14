import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Created by DanielLund on 3/11/17.
 */
public class AuthorAAVMapper extends Mapper<LongWritable, Text, Text, Text> {

    static ArrayList<String> authors;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        authors = readAuthors(context);
    }

    public void map(LongWritable key, Text value, Mapper.Context context) throws
            IOException, InterruptedException {
        String[] line = value.toString().split("\\s+");
        if(line.length == 2) {
            String term = line[0];
            String idf = line[1];
            for (String author : authors) {
                context.write(new Text(author + " " + term), new Text("#" + idf));
            }
        }else if(line.length == 3){
            String author = line[0];
            String term = line[1];
            String tf = line[2];
            context.write(new Text(author + " " + term), new Text(tf));
        }else{
            System.err.println("Incorrect input file");
        }

    }

    public ArrayList<String> readAuthors(Context context) throws IOException {
        ArrayList<String> authors = new ArrayList<>();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path("/PA2_author_count/part-r-00000");
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        Long lines = new Long(0);
        while (true){
            String line = reader.readLine();
            if(line == null)
                break;
            else
                authors.add(line.trim());
        }
        return authors;
    }
}
