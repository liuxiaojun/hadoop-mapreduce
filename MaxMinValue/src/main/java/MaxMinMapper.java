/**
 * Created by liuxiaojun on 15/10/20.
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxMinMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    private Text text = new Text("key");
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        context.write(text, new LongWritable(Long.parseLong(line)));
    }
}
