import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by DanielLund on 3/30/17.
 * Colorado State University
 * CS435
 */
public class MainClass {

    private static String WIKICSVPATH = "/PA3_CSV";

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        if(args.length != 1){
            System.err.println("Usage <jar file> <MainClass> <input_file>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        MRJob.job(conf, args[0], WIKICSVPATH, CSVMapper.class, CSVReducer.class, MainClass.class, false);

    }
}
