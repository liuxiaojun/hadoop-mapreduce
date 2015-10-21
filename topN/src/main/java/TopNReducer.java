/**
 * Created by liuxiaojun on 15/10/20.
 */
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<IntWritable, IntWritable, Text, Text> {

    int len;
    int[] top;
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        for(int i=len; i>0; i--) {
            context.write(new Text(String.valueOf(len-i+1)), new Text(String.valueOf(top[i])));
        }
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for(IntWritable value : values) {
            add(value.get());
        }
    }

    private void add(int i) {
        top[0] = i;
        Arrays.sort(top);
    }

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        len = context.getConfiguration().getInt("N", 10);
        top = new int[len + 1];
    }

}
