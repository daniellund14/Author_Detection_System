import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Created by DanielLund on 3/31/17.
 * Colorado State University
 * CS435
 */
public class WikiHBaseInputFormat extends TextInputFormat {

    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)  {
        return new WikiRecordReader();
    }
}
