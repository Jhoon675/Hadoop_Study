package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhdoop.suppot.AirlinePerformanceParser;

public class ArrDelayCountMapper 
	extends Mapper<LongWritable,
					Text, 
					Text, 
					IntWritable>{
	
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputkey = new Text();
	
	@Override
	protected void map(LongWritable key, 
			Text value, 
			Context context)
			throws IOException, InterruptedException {
		
		if(key.get() == 0 && value.toString().contains("YEAR")){
			return;
		}
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if(parser.getArrivalDelayTime() > 0) {
			outputkey.set(parser.getYear() + "," + parser.getMonth());
			context.write(outputkey, outputValue);
		}	
	}
}
