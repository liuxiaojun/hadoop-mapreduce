/**
 * Created by liuxiaojun on 15/10/20.
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxMinReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;

        for(LongWritable value : values) {
            if(value.get() > max) {
                max = value.get();
            }
            if(value.get() < min) {
                min = value.get();
            }
        }

        context.write(new Text("MAX"), new LongWritable(max));
        context.write(new Text("MIN"), new LongWritable(min));
    }
}
