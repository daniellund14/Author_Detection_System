import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;

/**
 * Created by DanielLund on 3/31/17.
 * Colorado State University
 * CS435
 */
public class WikiInputSplit extends InputSplit {

    @Override
    public long getLength() throws IOException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[0];
    }
}

