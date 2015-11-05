/**
 * Created by liuxiaojun on 15/11/5.
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReverseIndexReducer extends
        Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        for(Text text : values) {
            stringBuffer.append(text.toString()+";");
        }

        context.write(key, new Text(stringBuffer.toString()));
    }

}