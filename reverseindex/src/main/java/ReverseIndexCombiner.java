/**
 * Created by liuxiaojun on 15/11/5.
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReverseIndexCombiner extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int num = 0;
        for(Text text : values) {
            num += Integer.parseInt(text.toString());
        }

        String[] arr = key.toString().split(":");

        context.write(new Text(arr[0]), new Text(arr[1]+":"+num));
    }

}
