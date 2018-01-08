package com.mani.mrloganalyzer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] ary = value.toString().split(",");

        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
        Date date = null;
        try {
            date = formatter.parse(ary[1]);
        } catch (ParseException ex) {
        }
        if (date == null) {
            System.err.println("Date format issue..!!");
        } else {
            context.write(new Text(date.getDate() + "/" + date.getMonth() + 1 + "/" + (date.getYear() + 1900)), new IntWritable(1));
        }
    }
}
