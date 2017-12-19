package com.mani.mrmaxlengthword;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Manindar
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    String maxword = "";

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        if (key.toString().length() > maxword.length()) {
            maxword = key.toString();
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(maxword), new IntWritable(maxword.length()));
    }
}
