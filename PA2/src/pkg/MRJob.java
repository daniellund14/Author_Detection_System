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
import java.util.ArrayList;
import java.util.Optional;

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
        Job job = Job.getInstance(conf);
        job.setJarByClass(mainClass);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

        return job.isSuccessful();
    }

    public static boolean multipleInputsJob(Configuration conf, String inputPath1, String inputPath2, String outputPath, Class mapperClass, Class reducerClass, Class mainClass, Optional<Integer> optionalInt)throws  IOException, InterruptedException, ClassNotFoundException{
        Job job = Job.getInstance(conf);
        job.setJarByClass(mainClass);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        if(optionalInt.isPresent()){
            job.setNumReduceTasks(optionalInt.get());
        }

        MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

        return job.isSuccessful();
    }

    public static ArrayList<String> readAuthors(Configuration conf) throws IOException {
        ArrayList<String> authors = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(Main_Offline.AUTHOR_COUNT_PATH);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        while (true){
            String line = reader.readLine();
            if(line == null)
                break;
            else
                authors.add(line.replace("\t", ""));
        }
        return authors;
    }

    public static Integer countAuthors(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
       return countLines(conf);
    }

    private static Integer countLines(Configuration conf) throws IOException{
        Path outputPath = new Path(offline.Main_Offline.AUTHOR_PATH + "/part-r-00000");
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(outputPath)));
        Integer lines = new Integer(0);
        while (reader.readLine() != null) {
            lines++;
        }
        return lines;
    }

    public static ArrayList<IdfTerm> getIDFArray(Mapper.Context context) throws IOException{
        ArrayList<IdfTerm>  idf = new ArrayList<>();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(Main_Offline.IDF_PATH);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = reader.readLine();
        while(line != null){
            idf.add(new IdfTerm(line));
        }
        return idf;
    }
}
