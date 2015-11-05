/**
 * Created by liuxiaojun on 15/11/5.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultiOutPutMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if(null != line && 0 != line.length()) {
            String[] arr = line.split(",");
            context.write(new IntWritable(Integer.parseInt(arr[0])), value);

            // campid, value
        }
    }
}
