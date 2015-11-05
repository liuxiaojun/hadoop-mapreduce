/**
 * Created by liuxiaojun on 15/11/5.
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ReverseIndexMapper extends
        Mapper<LongWritable, Text, Text, Text> {

    private String fileName;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if(0 != line.length()) {
            StringTokenizer stringTokenizer = new StringTokenizer(line);
            while(stringTokenizer.hasMoreTokens()) {
                context.write(new Text(stringTokenizer.nextToken()+":"+fileName), new Text("1"));
            }
        }
    }

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String path = fileSplit.getPath().toString();
        fileName = path.substring(path.indexOf("file"));
    }
}

/*
            1）file1：
            MapReduce is simple
            2）file2：
            MapReduce is powerful is simple
            3）file3：
            Hello MapReduce bye MapReduce
            样例输出如下所示。

            MapReduce      file1.txt:1;file2.txt:1;file3.txt:2;
            is        　　　　file1.txt:1;file2.txt:2;
            simple        　  file1.txt:1;file2.txt:1;
            powerful   　　 file2.txt:1;
            Hello       　　 file3.txt:1;
            bye       　　   file3.txt:1;
*/