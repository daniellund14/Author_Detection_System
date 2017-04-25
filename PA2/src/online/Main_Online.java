package online;

import offline.Main_Offline;
import offline.UnigramAuthorMapper;
import offline.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import pkg.MRJob;

import java.io.IOException;
import java.util.Optional;

/**
 * Created by DanielLund on 3/14/17.
 * Colorado State University
 * CS435
 */
public class Main_Online {
    //Online paths for files

//    private static final String PATH = "/PA2_Test_Online";
    private static final String PATH = "/PA2_Online";

    private static final String UNIGRAM_PATH = PATH + "_unigram";
    private static final String TF_PATH = PATH + "_tf";
    private static final String TFIDF_PATH = PATH + "_tfidf";

    private static final String COSSIM1_PATH = PATH + "_cossim1";
    public static final String COSSIM2_PATH = PATH + "_cossim2";
    private static final String AAV_PATH = PATH + "_aav";

    //Offline paths for files
    private static final String OFFLINE_IDF_PATH = Main_Offline.IDF_PATH; /* Reuses IDF file from offline computation */
    private static final String OFFLINE_TFIDF_PATH = Main_Offline.TFIDF_PATH;


    public static void main(String[] args) throws  IOException, InterruptedException, ClassNotFoundException{
        if(args.length != 1){
            System.out.printf("Usage: <jar file> <MainClass> <input_file>\n");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        MRJob.job(conf, args[0], UNIGRAM_PATH, UnigramAuthorMapper.class, WordCountReducer.class, Main_Online.class, false);
        MRJob.job(conf, UNIGRAM_PATH, TF_PATH, offline.TFMapper.class, offline.TFReducer.class, Main_Online.class, false);

        MRJob.multipleInputsJob(conf, TF_PATH, OFFLINE_IDF_PATH, TFIDF_PATH, online.TFIDFMapper.class, online.TFIDFReducer.class, Main_Online.class, Optional.empty());

        MRJob.multipleInputsJob(conf, TFIDF_PATH, OFFLINE_TFIDF_PATH, COSSIM1_PATH, online.CosSimMapper.class, online.CosSimReducer.class, Main_Online.class, Optional.of(10));
        MRJob.job(conf, COSSIM1_PATH, COSSIM2_PATH, online.CosSimMapper2.class, CosSimReducer2.class, Main_Online.class, true);


        MRJob.printTopTen(conf);
    }

}
