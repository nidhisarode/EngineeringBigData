// filtering only by word 'theft' from the column 'Primary Type'


package initialAnalysis.filteringTypeOfCrime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Task5Driver {

        public static void main (String[] args) throws Exception{
            Path inputPath =  new Path(args[0]);
            Path outputDir = new Path(args[1]);

            Configuration conf = new Configuration();

            conf.set("filter_value",args[2]);

            Job job = Job.getInstance(conf, "myInstance");
            job.setJarByClass(Task5Mapper.class);
            job.setMapperClass(Task5Mapper.class);
            job.setReducerClass(Task5Reducer.class);
            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, inputPath);
            job.setInputFormatClass(TextInputFormat.class);


            FileOutputFormat.setOutputPath(job, outputDir);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileSystem hdfs = FileSystem.get(conf);

            if (hdfs.exists(outputDir)){
                hdfs.delete(outputDir, true);
            }

            int code = job.waitForCompletion(true) ? 0 : 1;
            System.exit(code);
        }
    }







