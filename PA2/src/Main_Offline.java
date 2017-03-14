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

public class Main_Offline {
	public static void main(String[] args) throws IOException, ClassNotFoundException,
	InterruptedException {
        if (args.length != 5) {
			System.out.printf("Usage: <input_file> <output dir1> <output dir2> <output dir3> <output dir4>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		UnigramJob(conf, args[0], args[1]);
        TFJob(conf, args[1], args[2]);
        conf.setLong("NUMBER_AUTHORS", countAuthors(conf, args[1]));
        IDFJob(conf, args[1], args[3]);
        System.exit(TFIDFJob(conf, args[2], args[3], args[4])? 0 : -1);
	}


	private static boolean UnigramJob(Configuration conf, String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException{
		Job unigram=Job.getInstance(conf);
		unigram.setJarByClass(Main_Offline.class);

		unigram.setMapperClass(UnigramAuthorMapper.class);
		unigram.setReducerClass(WordCountReducer.class);
		unigram.setOutputKeyClass(Text.class);
		unigram.setOutputValueClass(Text.class);
		unigram.setInputFormatClass(TextInputFormat.class);
		unigram.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(unigram, new Path(inputPath));
		FileOutputFormat.setOutputPath(unigram, new Path(outputPath));
        unigram.waitForCompletion(true);
		return unigram.isSuccessful();
	}

	private static boolean TFJob(Configuration conf, String inputPath, String outputPath)throws IOException, InterruptedException, ClassNotFoundException{
		Job tf = Job.getInstance(conf);
		tf.setJarByClass(Main_Offline.class);
		tf.setMapperClass(TFMapper.class);
		tf.setReducerClass(TFReducer.class);
		tf.setOutputKeyClass(Text.class);
		tf.setOutputValueClass(Text.class);
		tf.setInputFormatClass(TextInputFormat.class);
		tf.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(tf, new Path(inputPath));
		FileOutputFormat.setOutputPath(tf, new Path(outputPath));
        tf.waitForCompletion(true);
		return tf.isSuccessful();
	}

	private static boolean IDFJob(Configuration conf, String inputPath, String outputPath) throws  IOException, InterruptedException, ClassNotFoundException {
        Job idf = Job.getInstance(conf);
        idf.setJarByClass(Main_Offline.class);
        idf.setMapperClass(IDFMapper.class);
        idf.setReducerClass(IDFReducer.class);
        idf.setOutputKeyClass(Text.class);
        idf.setOutputValueClass(Text.class);
        idf.setInputFormatClass(TextInputFormat.class);
        idf.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(idf, new Path(inputPath));
        FileOutputFormat.setOutputPath(idf, new Path(outputPath));
        idf.waitForCompletion(true);
        return idf.isSuccessful();
    }

    private static boolean TFIDFJob(Configuration conf, String inputPath1, String inputPath2, String outputPath)throws  IOException, InterruptedException, ClassNotFoundException{
        Job tfidf = Job.getInstance(conf);
        tfidf.setJarByClass(Main_Offline.class);
        tfidf.setMapperClass(AuthorAAVMapper.class);
        tfidf.setReducerClass(AuthorAAVReducer.class);
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



	private static Long countAuthors(Configuration conf, String inputFile) throws IOException, ClassNotFoundException, InterruptedException {
		//conf.setLong("NUMBER_AUTHORS", 0);
		Job countAuthors = Job.getInstance(conf);

		countAuthors.setJarByClass(Main_Offline.class);
		countAuthors.setMapperClass(CountAuthorsMapper.class);
		countAuthors.setReducerClass(CountAuthorsReducer.class);
		countAuthors.setOutputKeyClass(Text.class);
		countAuthors.setOutputValueClass(Text.class);
		countAuthors.setInputFormatClass(TextInputFormat.class);
		countAuthors.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(countAuthors, new Path(inputFile));
		FileOutputFormat.setOutputPath(countAuthors, new Path("/PA2_author_count"));
		countAuthors.waitForCompletion(true);
		Path outputPath = new Path("/PA2_author_count/part-r-00000");
		FileSystem fs = FileSystem.get(conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(outputPath)));
        Long lines = new Long(0);
		while(reader.readLine() != null){
            lines++;
        }
		return lines;
	}
}