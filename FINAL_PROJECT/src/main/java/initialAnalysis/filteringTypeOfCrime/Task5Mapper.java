package initialAnalysis.filteringTypeOfCrime;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Task5Mapper extends Mapper<Object, Text, NullWritable,Text> {

    String filter;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        try {
            filter = context.getConfiguration().get("filter_value");
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try{
            String[] fields = value.toString().split(",");


            if((fields[6].trim().equalsIgnoreCase(filter))){
                context.write(NullWritable.get(),value);
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
