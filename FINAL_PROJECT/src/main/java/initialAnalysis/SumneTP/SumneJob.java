package initialAnalysis.SumneTP;

import finalAnalysis.yearlyTop5Districts.CompositeKeyWritable;
import finalAnalysis.yearlyTop5Districts.YearlyTop5Districts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SumneJob {
    public static class SumneMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            String[] string = value.toString().split(",");
            StringBuilder sb = new StringBuilder();

            for(int i=0; i<string.length; i++)
            {
                if(i!=3){
                    sb.append(string[i]+"\t");
                }

            }
            Text t = new Text(sb.toString());
            context.write(NullWritable.get(),t);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
       // conf.set("filter_value",args[3]);
        Job job = Job.getInstance(conf, "SumneJob");
        job.setJarByClass(SumneJob.class);
        job.setMapperClass(SumneMapper.class);

//        job.setCombinerClass(MovieReducer1.class);
        //job.setReducerClass(S.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

       // boolean complete = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true)? 0:1);


        }

    }
