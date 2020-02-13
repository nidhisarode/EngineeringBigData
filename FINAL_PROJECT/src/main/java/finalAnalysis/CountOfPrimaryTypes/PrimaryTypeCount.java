package finalAnalysis.CountOfPrimaryTypes;

import finalAnalysis.yearlyTop5Districts.BinningYearly;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PrimaryTypeCount {

    public static class PrimaryTypeCountMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {

                String[] fields = value.toString().split(",");
                if(!fields[5].equals("IUCR")){
                    context.write(new Text(fields[6]),new LongWritable(1));
                }

            }catch(Exception ex){
                ex.printStackTrace();
            }
        }
    }

    public static class PrimaryTypeCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        LongWritable count = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            try{
                long sum=0;
                for(LongWritable lw : values){
                    sum+=lw.get();
                }
                count.set(sum);
                context.write(key,count);
            }catch(Exception ex){
                ex.printStackTrace();
            }
        }
    }

    public static void main(String[] args){

      try {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Primary Types");


        job.setJarByClass(PrimaryTypeCount.class);


        //Assigning the mapper, reducer, combiner classes
        job.setMapperClass(PrimaryTypeCountMapper.class);
        job.setReducerClass(PrimaryTypeCountReducer.class);

        //Setting Input and Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);


        //Setting Number of Reducers
        job.setNumReduceTasks(1);

        //Specify Input Path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        //Specify Output Path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }catch(Exception ex){
        ex.printStackTrace();

    }
}
}
