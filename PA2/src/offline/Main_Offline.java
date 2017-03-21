package offline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import pkg.MRJob;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main_Offline {

    private static final String UNIGRAM_PATH = "/PA2_Offline_unigram";
    private static final String TF_PATH = "/PA2_Offline_tf";
    private static final String IDF_PATH = "/PA2_Offline_idf";
    private static final String TFIDF_PATH = "/PA2_Offline_tfidf";
    private static final String AAV_PATH = "/PA2_Offline_aav";
    public static final String AUTHOR_PATH = "/PA2_Offline_authors";
    public static final String AUTHOR_COUNT_PATH = "/PA2_Offline_authors/part-r-00000";
    public static final String NUMBER_AUTHORS = "NUMBER_AUTHORS";


    public static void main(String[] args) throws IOException, ClassNotFoundException,
	InterruptedException {
        if (args.length != 1) {
			System.out.printf("Usage: <jar file> <MainClass> <input_file>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		MRJob.job(conf, args[0], UNIGRAM_PATH, UnigramAuthorMapper.class, WordCountReducer.class, Main_Offline.class, false);
        MRJob.job(conf, UNIGRAM_PATH, TF_PATH, TFMapper.class, TFReducer.class, Main_Offline.class, false);
        conf.setInt(NUMBER_AUTHORS, countAuthors(conf, TF_PATH)); /* Sets number of authors as a property for configuration used in IDF calculations */
        MRJob.job(conf, TF_PATH, IDF_PATH, IDFMapper.class, IDFReducer.class, Main_Offline.class, false);
        MRJob.multipleInputsJob(conf, TF_PATH, IDF_PATH, TFIDF_PATH, TFIDFMapper.class, TFIDFReducer.class, Main_Offline.class);
       // MRJob.job(conf, TFIDF_PATH, AAV_PATH, AAVMapper.class, AAVReducer.class, Main_Offline.class, true);

	}

	private static Integer countAuthors(Configuration conf, String inputFile) throws IOException, ClassNotFoundException, InterruptedException {
        if(MRJob.job(conf, inputFile, AUTHOR_PATH, CountAuthorsMapper.class, CountAuthorsReducer.class, Main_Offline.class, false)) {
            Path outputPath = new Path(AUTHOR_PATH + "/part-r-00000");
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(outputPath)));
            Integer lines = new Integer(0);
            while (reader.readLine() != null) {
                lines++;
            }
            return lines;
        }else{
            return new Integer(-1);
        }
	}
}