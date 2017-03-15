package offline;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by DanielLund on 3/11/17.
 */
public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
    static String[] authors;
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
            //if(new Double(idf) != 0.0)
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

    public String[] readAuthors(Context context) throws IOException {

        String[] list = new String[(context.getConfiguration().getInt(Main_Offline.NUMBER_AUTHORS, -1))];
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(Main_Offline.AUTHOR_COUNT_PATH);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        Long lines = new Long(0);
        int index = 0;
        while (true){
            String line = reader.readLine();
            if(line == null)
                break;
            else
                list[index] = line.trim();
            index++;
        }
        return list;
    }
}
