/**
 * Created by liuxiaojun on 15/9/24.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewMaxTemperatureMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println("key: " + key);
        String year = line.substring(15, 19);
        int airTemperature;
        if (line.charAt(45) == '+') {
            airTemperature = Integer.parseInt(line.substring(46, 50));
        } else {
            airTemperature = Integer.parseInt(line.substring(45, 50));
        }

        String quality = line.substring(50, 51);
        System.out.println("quality: " + quality);

        if (airTemperature != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
