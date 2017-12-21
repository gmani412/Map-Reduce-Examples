package com.mani.mraggregateexample;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Manindar
 */
public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;
        Iterator itr = values.iterator();
        while (itr.hasNext()) {
            sum += ((DoubleWritable) itr.next()).get();
            count++;
        }
        context.write(key, new DoubleWritable(sum / count));
    }
}
