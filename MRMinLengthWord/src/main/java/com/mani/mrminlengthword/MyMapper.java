package com.mani.mrminlengthword;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Manindar
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreElements()) {
            String word = (String) tokenizer.nextElement();
            if (word.isEmpty() || word.trim().isEmpty()) {
                continue;
            }
            context.write(new Text(word), new IntWritable(word.length()));
        }
    }
}
