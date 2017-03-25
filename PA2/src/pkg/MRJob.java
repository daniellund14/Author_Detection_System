package pkg;

import offline.Main_Offline;
import online.CosSimCombiner;
import online.Main_Online;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
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
        if(combiner){
            job.setCombinerClass(CosSimCombiner.class);
        }

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
                authors.add(line.trim());
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

    public static void printTopTen(Configuration conf)throws IOException{
        ArrayList<AuthorCosSim>  cosSimArray = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(Main_Online.COSSIM2_PATH + "/part-r-00000");
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = reader.readLine();
        while(line != null){
            cosSimArray.add(new AuthorCosSim(line));
            line = reader.readLine();
        }
        Collections.sort(cosSimArray,Collections.reverseOrder());
        //System.out.println(cosSimArray.toString());
        Integer count = 0;

        String Dashes = new String(new char[80]).replace("\0", "-");
        System.out.println("\nTop 10 Author list");
        System.out.println(Dashes);
        for(AuthorCosSim sim: cosSimArray){
            if(count == 10){
                break;
            }
            System.out.printf("%d: %s\n", count + 1, sim);
            count++;
        }

        System.out.println("\n" + Dashes);



    }
}
