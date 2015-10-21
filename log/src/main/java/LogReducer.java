/**
 * Created by liuxiaojun on 15/10/21.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        long num = 0L;
        for(IntWritable value : values) {
            num += value.get();
        }
        context.write(key, new LongWritable(num));
    }

}