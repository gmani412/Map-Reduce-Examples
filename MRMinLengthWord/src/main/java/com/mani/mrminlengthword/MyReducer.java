package com.mani.mrminlengthword;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Manindar
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    String minword = "";

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        if (minword.length() == 0) {
            minword = key.toString();
        } else if (key.toString().length() < minword.length()) {
            minword = key.toString();
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(minword), new IntWritable(minword.length()));
    }
}
