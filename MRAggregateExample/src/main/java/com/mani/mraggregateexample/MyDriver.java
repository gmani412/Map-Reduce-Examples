package com.mani.mraggregateexample;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * To run this program you can execute the below command. ----------------------
 * hadoop jar MRAggregateExample-*.jar com.mani.mraggregateexample.MyDriver
 * /tmp/mani/employee_data.tsv /tmp/op
 */
/**
 * The required input data is available under files/employee_data.tsv file. This
 * Map-reduce job will calculate the "average salary" based on input passed. For
 * which you should pass either "gender" or "designation" or "branch" as a 3rd
 * parameter to the program along with the input & output file paths. In case of
 * wrong input by defult it will work for "branch". The input is tab delimited
 * data, and code is written accordingly.-------------------------------------
 * ------------------------- Input Schema Details ----------------------------
 * ------- EmpId, Fname, Lname, Gender, DOJ, Designation, Salary, Branch -----
 * -------------------------------------------------------- 4th index -> Gender
 * --------------------------------------------------- 6th index -> Designation
 * -------------------------------------------------------- 7th index -> Salary
 * -------------------------------------------------------- 8th index -> Branch
 *
 * @author Manindar
 */
public class MyDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args == null || args.length == 0 || args.length < 2
                || args[0] == null || args[0].isEmpty()
                || args[1] == null || args[1].isEmpty()
                || args[2] == null || args[2].isEmpty()) {
            System.out.println("Pass 2 arguments. 1) input hdfs path 2) output hdfs path 3) aggrigation ('gender' or 'designation' or 'branch')");
            System.exit(0);
        }
        Configuration conf = new Configuration();
        conf.set("aggrigation", args[2].toLowerCase().trim());
        Job job = new Job(conf, "Aggrigation example using Map reduce");
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
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
