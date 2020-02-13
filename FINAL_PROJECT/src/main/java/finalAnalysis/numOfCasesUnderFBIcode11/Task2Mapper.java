package finalAnalysis.numOfCasesUnderFBIcode11;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import finalAnalysis.numOfCasesUnderFBIcode11.Task2Driver.MyCounters;

public class Task2Mapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	private Text fbiCode;
	private final static LongWritable ONE = new LongWritable(1); 

	
	@Override
	public void setup(Context context)
	{
		fbiCode = new Text();
	}
	
	@Override
	public void map(LongWritable key,Text crimeRecord,Context context) throws IOException, InterruptedException
	{
		String strValue = crimeRecord.toString();
		System.out.println("MapperClass : Current record is [ "+strValue+" ]");
		
		String[] split = strValue.split(",");
		
		if( split.length<16 || split[2].equals("") || split[15].equals("") )
		{
			System.out.println("MapperClass : Invalid record found");
			context.getCounter(MyCounters.INVALIDRECORDS).increment(1);
		}
		else if (split[15].equals("11"))
		{
			context.getCounter(MyCounters.REQUIREDRECORDS).increment(1);
			
			String strFbiCode = split[15];
			
			System.out.println("MapperClass : Inserting ["+strFbiCode+" , 1] in the context");
			
			fbiCode.set(strFbiCode);
			context.write(fbiCode, ONE);
		}
	}

	
}
