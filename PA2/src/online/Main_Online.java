package online;

import offline.TFMapper;
import offline.TFReducer;
import offline.UnigramAuthorMapper;
import offline.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import pkg.MRJob;

import java.io.IOException;

/**
 * Created by DanielLund on 3/14/17.
 * Colorado State University
 * CS435
 */
public class Main_Online {
    private static final String UNIGRAM_PATH = "/PA2_Online_unigram";
    private static final String TF_PATH = "/PA2_Online_tf";
    private static final String IDF_PATH = "/PA2_Offline_idf"; /* Reuses IDF file from offline computation */
    private static final String AAV_PATH = "/PA2_Online_aav";
    private static final String AAV_OFFLINE = "/PA2_Offline_aav";



    public static final String DASHES = new String(new char[80]).replace("\0", "-");
    public static void main(String[] args) throws  IOException, InterruptedException, ClassNotFoundException{
        if(args.length != 1){
            System.out.printf("Usage: <jar file> <MainClass> <input_file>\n");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        MRJob.job(conf, args[0], UNIGRAM_PATH, UnigramAuthorMapper.class, WordCountReducer.class, Main_Online.class, false);
        MRJob.job(conf, UNIGRAM_PATH, TF_PATH, TFMapper.class, TFReducer.class, Main_Online.class, false);
        MRJob.multipleInputsJob(conf, TF_PATH, IDF_PATH, AAV_PATH, TFIDFMapper.class, TFIDFReducer.class, Main_Online.class);

        /*
        TODO Implement MR job to calculate cosine similarity, possibly consider a method? depending on how long the code is
            MRJob.job(conf, AAV_PATH, AAV_OFFLINE, CosSimMapper.class, CosSimReducer.class, Main_Online.class);
        */
        

    }


}
