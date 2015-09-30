import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IntSortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {


    //IntWritable val = new IntWritable(1);

    private Text val = new Text("");
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.trim().length()>0){
            context.write(new IntWritable(Integer.valueOf(value.toString())), val);
        }
    }
}