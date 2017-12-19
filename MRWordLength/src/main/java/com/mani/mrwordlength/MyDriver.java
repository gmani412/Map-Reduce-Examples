package com.mani.mrwordlength;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * To run this program you can execute the below command ----------------------
 * hadoop jar MRWordLength-*.jar com.mani.mrwordlength.MyDriver
 * /tmp/mani/sample.txt /tmp/op
 *
 *
 * @author Manindar
 */
public class MyDriver {

    /**
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args == null || args.length == 0 || args.length < 1
                || args[0] == null || args[0].isEmpty()
                || args[1] == null || args[1].isEmpty()) {
            System.out.println("Pass 2 arguments. 1) input hdfs path 2) output hdfs path");
            System.exit(0);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Length");
        job.setJarByClass(MyDriver.class);
        job.setMapperClass(MyMapper.class);
        /**
         * Here i am not using any reducer class for demo purpose. You can use
         * it further to avoid printing redundant word in output.
         */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
