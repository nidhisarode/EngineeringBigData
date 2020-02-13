package finalAnalysis.numOfCasesUnderFBIcode11;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<Text, LongWritable, Text, LongWritable> 
{

	@Override
	public void reduce(Text fbiCode,Iterable<LongWritable> cases,Context context) throws IOException, InterruptedException
	{
		long count  = 0;
		for (LongWritable caseNo : cases) 
		{
			count = count + caseNo.get();
		}

		System.out.println("ReducerClass : Inserting ["+fbiCode.toString()+" , "+count);
		context.write(fbiCode, new LongWritable(count));
	}


}
