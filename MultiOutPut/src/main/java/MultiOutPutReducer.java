/**
 * Created by liuxiaojun on 15/11/5.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultiOutPutReducer extends
        Reducer<IntWritable, Text, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> multipleOutputs = null;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for(Text text : values) {
            multipleOutputs.write("KeySpilt", NullWritable.get(), text, key.toString()+"/");
            multipleOutputs.write("AllPart", NullWritable.get(), text);
        }
    }

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        if(null != multipleOutputs) {
            multipleOutputs.close();
            multipleOutputs = null;
        }
    }


}