package com.mani.mrminlengthword;

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
 * hadoop jar MRMinLengthWord-*.jar com.mani.mrminlengthword.MyDriver
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
        Job job = new Job(conf, "Min Length Word");
        job.setJarByClass(MyDriver.class);
        job.setMapperClass(MyMapper.class);
        /**
         * We can mention reducer as combiner class in case of huge dataset to
         * work with. For small datasets combiner (mini-reducer at mapper side)
         * is not required.
         */
//        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
