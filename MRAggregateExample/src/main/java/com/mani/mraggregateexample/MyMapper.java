package com.mani.mraggregateexample;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Manindar
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String aggrigation = conf.get("aggrigation");
        int salaryPos = 6;
        int inputPos;
        switch (aggrigation) {
            case "gender":
                inputPos = 3;
                break;
            case "designation":
                inputPos = 5;
                break;
            case "branch":
                inputPos = 7;
                break;
            default:
                inputPos = 7;
                break;
        }
        String[] columns = value.toString().split("\\t");
        context.write(new Text(columns[inputPos]), new DoubleWritable(Double.parseDouble(columns[salaryPos])));
    }
}
