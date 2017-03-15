package online;

import offline.*;


import offline.TFIDFMapper;
import offline.TFIDFReducer;
import org.apache.hadoop.conf.Configuration;
import pkg.MRJob;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by DanielLund on 3/14/17.
 * Colorado State University
 * CS435
 */
public class Main_Online {
    private static final String UNIGRAM_PATH = "/PA2_Online_unigram";
    private static final String TF_PATH = "/PA2_Online_tf";
    private static final String IDF_PATH = "/PA2_Offline_idf"; /* Reuses IDF file from offline computation */
    private static final String AAV_PATH = "/PA2_Online_AAV";



    public static final String DASHES = new String(new char[80]).replace("\0", "-");
    public static void main(String[] args) throws  IOException, InterruptedException, ClassNotFoundException{
        Scanner stdin = new Scanner(System.in);
        System.out.println("\n" + DASHES);
        System.out.println("Author Detection Progrom");
        System.out.println(DASHES + "\n");
        System.out.print("Enter file path to detect authorship:");
        String inputFile = stdin.nextLine();
        Configuration conf = new Configuration();
        MRJob.job(conf, inputFile, UNIGRAM_PATH, UnigramAuthorMapper.class, TFReducer.class, Main_Online.class);
        MRJob.job(conf, UNIGRAM_PATH, TF_PATH, TFMapper.class, TFReducer.class, Main_Online.class);
        MRJob.multipleInputsJob(conf, TF_PATH, IDF_PATH, AAV_PATH, TFIDFMapper.class, TFIDFReducer.class, Main_Online.class);
    }





}
