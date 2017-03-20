package pkg;

import offline.Main_Offline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by DanielLund on 3/14/17.
 * Colorado State University
 * CS435
 *
 * This class is created to provide utility functions for MR jobs, whether it be creating MR jobs
 * or reading files from the Hadoop FS with read authors. Other utilities will be provided
 */
public class MRJob {

    public static boolean job(Configuration conf, String inputPath, String outputPath, Class mapperClass, Class reducerClass, Class mainClass, boolean combiner) throws IOException, InterruptedException, ClassNotFoundException {
        Job idf = Job.getInstance(conf);
        idf.setJarByClass(mainClass);
        idf.setMapperClass(mapperClass);
        idf.setReducerClass(reducerClass);
        idf.setOutputKeyClass(Text.class);
        idf.setOutputValueClass(Text.class);
        idf.setInputFormatClass(TextInputFormat.class);
        idf.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(idf, new Path(inputPath));
        FileOutputFormat.setOutputPath(idf, new Path(outputPath));

        idf.waitForCompletion(true);

        return idf.isSuccessful();
    }

    public static boolean multipleInputsJob(Configuration conf, String inputPath1, String inputPath2, String outputPath, Class mapperClass, Class reducerClass, Class mainClass)throws  IOException, InterruptedException, ClassNotFoundException{
        Job tfidf = Job.getInstance(conf);
        tfidf.setJarByClass(mainClass);
        tfidf.setMapperClass(mapperClass);
        tfidf.setReducerClass(reducerClass);
        tfidf.setOutputKeyClass(Text.class);
        tfidf.setOutputValueClass(Text.class);
        tfidf.setInputFormatClass(TextInputFormat.class);
        tfidf.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(tfidf, new Path(inputPath1), TextInputFormat.class);
        MultipleInputs.addInputPath(tfidf, new Path(inputPath2), TextInputFormat.class);
        FileOutputFormat.setOutputPath(tfidf, new Path(outputPath));

        tfidf.waitForCompletion(true);

        return tfidf.isSuccessful();
    }

    public static String[] readAuthors(Mapper.Context context) throws IOException {
        String[] list = new String[(context.getConfiguration().getInt(Main_Offline.NUMBER_AUTHORS, -1))];
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(Main_Offline.AUTHOR_COUNT_PATH);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        int index = 0;
        while (true){
            String line = reader.readLine();
            if(line == null)
                break;
            else
                list[index] = line.replace("\t", "");
            index++;
        }
        return list;
    }
}
