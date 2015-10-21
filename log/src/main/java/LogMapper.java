/**
 * Created by liuxiaojun on 15/10/21.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        String outKey = findKey(line);
        context.write(new Text(outKey), new IntWritable(1));
    }

    private String findKey(String line) {
        String result = null;
        if(line.length() > 0) {
            if(line.indexOf("GET") > -1) {
                result = line.substring(line.indexOf("GET"), line.indexOf("HTTP/1.0"));
            } else if(line.indexOf("POST") > -1) {
                result = line.substring(line.indexOf("POST"), line.indexOf("HTTP/1.0"));
            }
        }
        return result;
    }
}
