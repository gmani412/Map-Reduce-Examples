package com.mani.mrloganalyzer;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * To run this program you can execute the below command. ----------------------
 * hadoop jar MRLogAnalyzer-*.jar com.mani.mrloganalyzer.MyDriver
 * /tmp/mani/employee_data.tsv /tmp/op
 */
/**
 * The required input data is available under files/logAnalyzer.txt file. This
 * Map-reduce job will analyze the log data and find out how many hits happened
 * to the server per day.
 *
 * @author Manindar
 */
public class MyDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args == null || args.length == 0 || args.length < 2
                || args[0] == null || args[0].isEmpty()
                || args[1] == null || args[1].isEmpty()) {
            System.out.println("Pass 2 arguments. 1) input hdfs path 2) output hdfs path");
            System.exit(0);
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Log analyzer example using Map reduce");
        job.setJarByClass(MyDriver.class);
        job.setMapperClass(MyMapper.class);
        /**
         * We can mention reducer as combiner class in case of huge dataset to
         * work with. For small datasets combiner (mini-reducer at mapper side)
         * is not required.
         */
//        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
