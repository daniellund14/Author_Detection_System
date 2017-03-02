import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
public class MainClass {
	public static void main(String[] args) throws IOException, ClassNotFoundException,
	InterruptedException {
		if (args.length != 3) {
			System.out.printf("Usage: <jar file> <file_type> <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Configuration conf =new Configuration();
		
		Job job1=Job.getInstance(conf);
		job1.setJarByClass(MainClass.class);

		job1.setMapperClass(UnigramAuthorMapper.class);
		job1.setReducerClass(WordCountReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		if(job1.isSuccessful()) {
			Job job2 = Job.getInstance(conf);
            job2.setJarByClass(MainClass.class);
            job2.setMapperClass(TermFrequencyMapper.class);
            job2.setReducerClass(TermFrequencyReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}


	}
}