package finalAnalysis.yearlyTop5Districts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class BinningYearly {

    public static class BinningMapper extends Mapper<LongWritable, Text, Text,NullWritable>{
        private MultipleOutputs<Text,NullWritable> mos = null;
        SimpleDateFormat frmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");

            @Override
            protected void setup(Context context) throws IOException, InterruptedException {
                mos = new MultipleOutputs<Text, NullWritable>(context);
            }

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] fields = value.toString().split(",");
                try {
                    if (!fields[3].equals("Date")) {
                        Date crimeDate = frmt.parse(fields[3]);
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(crimeDate);

                        int year = cal.get(Calendar.YEAR);


                        if (year == 2012) {
                            mos.write("bins", value, NullWritable.get(), "2012data");
                        }
                        if (year == 2013) {
                            mos.write("bins", value, NullWritable.get(), "2013data");
                        }
                        if (year == 2014) {
                            mos.write("bins", value, NullWritable.get(), "2014data");
                        }
                        if (year == 2015) {
                            mos.write("bins", value, NullWritable.get(), "2015data");
                        }
                        if (year == 2016) {
                            mos.write("bins", value, NullWritable.get(), "2016data");
                        }
                        if (year == 2017) {
                            mos.write("bins", value, NullWritable.get(), "2017data");
                        }

                    }
                }
                catch(Exception ex){
                        ex.printStackTrace();
                    }


                }


            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException {
                mos.close();
            }
        }

        public static void main(String args[]){

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Chaining Binning");


            job.setJarByClass(BinningYearly.class);


            //Assigning the mapper, reducer, combiner classes
            job.setMapperClass(BinningMapper.class);

            //Setting Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
            MultipleOutputs.setCountersEnabled(job, true);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);


            //Setting Number of Reducers
            job.setNumReduceTasks(0);

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

