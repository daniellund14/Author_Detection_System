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
        Integer fileType = new Integer(args[0]);

		job1.setMapperClass(UnigramAuthorMapper.class);
		job1.setReducerClass(WordCountReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf);

		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}