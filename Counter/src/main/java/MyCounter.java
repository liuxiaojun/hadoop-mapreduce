import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.math.util.OpenIntToDoubleHashMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;  //counter
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyCounter {
    public static class MyCounterMap extends Mapper<LongWritable, Text, Text, Text> {
        private Text val = new Text("");

        public static Counter ct = null;

        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            if (line.trim().length()>0){
                if (line.trim().length() > 2) {
                    ct = context.getCounter("ErrorCounter", "toolong");
                    ct.increment(1);
                } else if (line.trim().length() < 2) {
                    ct = context.getCounter("ErrorCounter", "tooshort");
                    ct.increment(1);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.print("Usage: MaxTemperature<input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(MyCounter.class);
        job.setMapperClass(MyCounterMap.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        CounterGroup counterGroup = job.getCounters().getGroup("ErrorCounter");
        Iterator<Counter> counters = counterGroup.iterator();

        System.out.println("===============");
        while (counters.hasNext()){
            Counter counter = counters.next();
            System.out.println(counter.getName());

            System.out.println(job.getCounters().findCounter("ErrorCounter",counter.getName()).getValue());

        }


        //System.exit( ? 0 : 1);
    }
}