package offline;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pkg.MRJob;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by DanielLund on 3/22/17.
 * Colorado State University
 * CS435
 */
public class TFIDFReducer extends Reducer<Text,Text,Text,Text> {
    static ArrayList<String> authors;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        authors = MRJob.readAuthors(context.getConfiguration());
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        ArrayList<Text> cache = new ArrayList<>();
        ArrayList<String> usedAuthors = new ArrayList<>();
        Double IDF = 0.0;
        Double TF = 0.5;
        for(Text s: values){
           if(s.toString().contains("#")){
                IDF = new Double(s.toString().replace("#", ""));
            }else{
               cache.add(s);
            }
        }
        ArrayList<String> writtenAuthors = new ArrayList<>();
        for(Text s: cache){
            if(s.toString().contains("#")){
                continue;
            }
            String[] value = s.toString().split(",");
            String author = value[0];
            TF = new Double(value[1]);
            usedAuthors.add(author);
            Double TFxIDF = TF * IDF;
            if(!writtenAuthors.contains(author)) {
                context.write(new Text(author + " " + key.toString()), new Text(IDF.toString()));
                writtenAuthors.add(author);
            }
        }

        for(String author: authors){
            if(!usedAuthors.contains(author)){
                Double TFxIDF = .5 * IDF;
                if(!writtenAuthors.contains(author)) {
                    context.write(new Text(author + " " + key.toString()), new Text(TFxIDF.toString()));
                    writtenAuthors.add(author);
                }
            }
        }

    }
}
