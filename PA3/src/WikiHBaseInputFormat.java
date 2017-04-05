import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by DanielLund on 3/31/17.
 * Colorado State University
 * CS435
 */
public class WikiHBaseInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new WikiRecordReader();
    }
}
