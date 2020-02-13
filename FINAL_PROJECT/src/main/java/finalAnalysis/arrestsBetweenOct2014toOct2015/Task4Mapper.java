package finalAnalysis.arrestsBetweenOct2014toOct2015;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import finalAnalysis.arrestsBetweenOct2014toOct2015.Task4Driver.MyCounters;


public class Task4Mapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	private final static LongWritable ONE = new LongWritable(1); 
	private final static Text CASE = new Text("CASE");

	
	@Override
	public void map(LongWritable key,Text crimeRecord,Context context) throws IOException, InterruptedException
	{
		String strValue = crimeRecord.toString();
		System.out.println("MapperClass : Current record is [ "+strValue+" ]");
		
		String[] split = strValue.split(",");
		
		if( split.length<9 || split[3].equals("") || split[9].equals("") )
		{
			System.out.println("MapperClass : Invalid record found");
			context.getCounter(MyCounters.INVALIDRECORDS).increment(1);
		}
		else if ( split[9].trim().equalsIgnoreCase("TRUE"))
		{
			boolean validDate  = false;
			try 
			{
				validDate = checkDate(split[2]);
			} 
			catch (ParseException e)
			{
				context.getCounter(MyCounters.INVALIDRECORDS).increment(1);
			}
			if (validDate) 
			{
				context.getCounter(MyCounters.REQUIREDRECORDS).increment(1);
			
				context.write(CASE, ONE);
				
			}	
		}
	}


	private boolean checkDate(String strDate) throws ParseException 
	{	
		Date startDate =  new SimpleDateFormat("MM/dd/yyyyy").parse("10/01/2014");
		Date endDate =  new SimpleDateFormat("MM/dd/yyyyy").parse("11/01/2015");
		
		Date currentDate =  new SimpleDateFormat("MM/dd/yyyyy HH:mm a").parse(strDate);
		
		if (currentDate.compareTo(startDate) >= 0  && currentDate.compareTo(endDate)<0 )
		{
			return true;
		}
		return false;
	}

	
}
