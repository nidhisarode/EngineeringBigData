package finalAnalysis.numberOfArrestsUnderEachFBIcode;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3Reducer extends Reducer<Text, LongWritable, Text, LongWritable> 
{

	@Override
	public void reduce(Text district,Iterable<LongWritable> listOfCnts,Context context) throws IOException, InterruptedException
	{
		long count  = 0;
		for (LongWritable cnt : listOfCnts) 
		{
			count = count + cnt.get();
		}

		System.out.println("ReducerClass : Inserting ["+district.toString()+" , "+count);
		context.write(district, new LongWritable(count));
	}


}
