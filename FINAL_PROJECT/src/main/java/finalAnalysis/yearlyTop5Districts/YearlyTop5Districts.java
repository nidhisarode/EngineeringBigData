package finalAnalysis.yearlyTop5Districts;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class YearlyTop5Districts {

    public static class FrequencyByDistrictMapper extends Mapper<Object, Text, Text, LongWritable> {
        SimpleDateFormat frmt = new SimpleDateFormat("MM/dd/yyyyy HH:mm:ss a");
        LongWritable one = new LongWritable(1);
        int yearFilter;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                yearFilter = Integer.parseInt(context.getConfiguration().get("filter_value"));
            }catch(Exception ex){
                ex.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] separatedInput = value.toString().split(",");
            try {
                Date crimeDate = frmt.parse(separatedInput[3]);
                Calendar cal = Calendar.getInstance();
                cal.setTime(crimeDate);
                int year = cal.get(Calendar.YEAR);


            String crimeString = separatedInput[13];
            if (crimeString == null) {
                return;
            }
            if (yearFilter == year) {
                // The foreign join key is the user ID
                Text outKey = new Text(crimeString);
                context.write(outKey, one);
            }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static 	class FrequencyByDistrictReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            LongWritable result = new LongWritable();
            long count = 0;
            for(LongWritable val :values){
                count++;
            }
            result.set(count);
            context.write(key,result);
        }
    }

    public static class FrequencyMapper2 extends Mapper<LongWritable, Text, CompositeKeyWritable, IntWritable>{

        @Override
        protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            Text districtId = new Text();
            LongWritable count = new LongWritable();
            if(values.toString().length()>0)
            {
                try{
                    String str[] = values.toString().split("\t");
                    districtId.set(str[0]);
                    count.set(Long.parseLong(str[1]));

                    CompositeKeyWritable cw = new CompositeKeyWritable(districtId.toString(), count.get());
                    context.write(cw, new IntWritable(1));
                }catch(Exception ex){
                    Logger.getLogger(FrequencyMapper2.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static class FrequencyReducer2 extends Reducer<CompositeKeyWritable, IntWritable, CompositeKeyWritable, IntWritable> {

        public static int count = 0;

        @Override
        protected void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {

                if (count < 5) {
                    context.write(key, new IntWritable(count));
                    count++;
                } else {
                    break;
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("filter_value",args[3]);
        Job job = Job.getInstance(conf, "FrequencyByDistrict");
        job.setJarByClass(YearlyTop5Districts.class);
        job.setMapperClass(FrequencyByDistrictMapper.class);

//        job.setCombinerClass(MovieReducer1.class);
        job.setReducerClass(FrequencyByDistrictReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean complete = job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true)? 0:1);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Chaining");

        if (complete) {
            job2.setJarByClass(YearlyTop5Districts.class);
            job2.setMapperClass(FrequencyMapper2.class);
//            job2.setCombinerClass(MovieReducer2.class);
            job2.setReducerClass(FrequencyReducer2.class);
            job2.setOutputKeyClass(CompositeKeyWritable.class);
            job2.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }

    }

}
