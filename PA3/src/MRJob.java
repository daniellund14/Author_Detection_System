import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
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
        job.setInputFormatClass(WikiHBaseInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));


        job.waitForCompletion(true);

        return job.isSuccessful();
    }

    public static boolean multipleInputsJob(Configuration conf, String inputPath1, String inputPath2, String outputPath, Class mapperClass, Class reducerClass, Class mainClass, Optional<Integer> optionalInt) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(mainClass);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        if (optionalInt.isPresent()) {
            job.setNumReduceTasks(optionalInt.get());
        }

        MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

        return job.isSuccessful();
    }

}
