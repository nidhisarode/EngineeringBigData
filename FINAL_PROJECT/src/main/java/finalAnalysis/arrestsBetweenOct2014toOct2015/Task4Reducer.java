package finalAnalysis.arrestsBetweenOct2014toOct2015;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task4Reducer extends Reducer<Text, LongWritable, Text, LongWritable> 
{

	@Override
	public void reduce(Text key,Iterable<LongWritable> listOfCnts,Context context) throws IOException, InterruptedException
	{
		long count  = 0;
		for (LongWritable cnt : listOfCnts) 
		{
			count = count + cnt.get();
		}

		System.out.println("ReducerClass : Inserting ["+key.toString()+" , "+count);
		context.write(key, new LongWritable(count));
	}


}
