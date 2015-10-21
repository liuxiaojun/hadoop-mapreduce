/**
 * Created by liuxiaojun on 15/10/20.
 */
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    int len;
    int[] top;
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        for(int i=1; i<top.length; i++) {
            context.write(new IntWritable(top[i]), new IntWritable(top[i]));
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] arr = line.split(",");
        if(4 == arr.length) {
            int payment = Integer.parseInt(arr[2]);
            add(payment);
        }
    }

    private void add(int payment) {
        top[0] = payment;
        Arrays.sort(top);
    }

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        len = context.getConfiguration().getInt("N", 10);
        top = new int[len+1];
    }

}
