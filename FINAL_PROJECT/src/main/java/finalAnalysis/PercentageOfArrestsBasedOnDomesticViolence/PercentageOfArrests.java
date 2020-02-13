package finalAnalysis.PercentageOfArrestsBasedOnDomesticViolence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class PercentageOfArrests {

    public static class PercentageOfArrestsMapper extends Mapper<LongWritable, Text,IntWritable, IntWritable> {
        SimpleDateFormat frmt = new SimpleDateFormat("MM/dd/yy HH:mm");
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                int num=0;
                String[] fields = value.toString().split(",");
                if(!fields[5].equals("IUCR")){

                    Date crimeDate = frmt.parse(fields[3]);
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(crimeDate);

                    int year = cal.get(Calendar.YEAR);
                    if(fields[9].equals("TRUE") && fields[10].equals("TRUE")){
                        num=1;
                    }
                    else {
                        num=0;
                    }
                    IntWritable year1 = new IntWritable(year);
                    IntWritable arrests = new IntWritable(num);

                    context.write(year1,arrests);
                }

            }catch(Exception ex){
                ex.printStackTrace();
            }
        }
    }

    public static class PercentageOfArrestsReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {

        @Override
        protected void reduce(IntWritable keyIn, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalcrimes = 0;
            int totalarrests = 0;
            for (IntWritable val : values) {
                totalarrests += val.get();
                totalcrimes = totalcrimes + 1;
            }
            float percentageOfPositiveReviews = ((float) totalarrests / totalcrimes) * 100;
            System.out.println(keyIn +" "+percentageOfPositiveReviews);
            context.write(keyIn, new DoubleWritable(percentageOfPositiveReviews));
        }

    }

    public static void main(String[] args){

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Count Primary Types");


            job.setJarByClass(PercentageOfArrests.class);


            //Assigning the mapper, reducer, combiner classes
            job.setMapperClass(PercentageOfArrestsMapper.class);
            job.setReducerClass(PercentageOfArrestsReducer.class);

            //Setting Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(DoubleWritable.class);


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
