package pkg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by DanielLund on 3/14/17.
 * Colorado State University
 * CS435
 */
public class MRJob {

    public static boolean job(Configuration conf, String inputPath, String outputPath, Class mapperClass, Class reducerClass, Class mainClass) throws IOException, InterruptedException, ClassNotFoundException {
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
}
