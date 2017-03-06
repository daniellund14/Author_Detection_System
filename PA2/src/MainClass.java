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
		if (args.length != 4) {
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
            job2.setMapperClass(TFMapper.class);
            job2.setReducerClass(TFReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			job2.waitForCompletion(true);

//			System.exit(job2.waitForCompletion(true) ? 0 : 1);
			if(job2.isSuccessful()){
				//TODO implement method to count number of authors
					conf.setLong("NUMBER_AUTHORS", 21);

				Job job3 = Job.getInstance(conf);
				job3.setJarByClass(MainClass.class);
				job3.setMapperClass(IDFMapper.class);
				job3.setReducerClass(IDFReducer.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				job3.setInputFormatClass(TextInputFormat.class);
				job3.setOutputFormatClass(TextOutputFormat.class);

				FileInputFormat.setInputPaths(job3, new Path(args[2]));
				FileOutputFormat.setOutputPath(job3, new Path(args[3]));

				job3.waitForCompletion(true);
			}
		}


	}
}