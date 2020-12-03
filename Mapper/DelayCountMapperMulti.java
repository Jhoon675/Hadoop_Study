package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhdoop.support.AirlinePerformanceParser;

public class DelayCountMapperMulti 
	extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private final static IntWritable outputValue = new IntWritable(1);
	
	private Text outputKey = new Text();

	@Override
	protected void map(LongWritable key, 
			Text value,
			Context context)
			throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		// 출력1: D,년도,월 -> 리듀서에서 출발 지연 MultipleOutputs로 출력
		// 출력2: A,년도,월 -> 리듀서에서 도착 지연 MultipleOutputs로 출력
		
		// 출발 지연시간 > 0 -> 키를 D,년,월
		if (parser.getDepartureDelayTime() > 0) {
			// 출력 키(출발지연 정보를 추가해서 키를 만들자)
			outputKey.set("D," + parser.getYear() + "," + parser.getMonth());
			context.write(outputKey, outputValue);
		} 
		// 도착 지연시간 > -> A,년,월
		if (parser.getArrivalDelayTime() > 0) {
			// 출력 키(도착지연 정보를 추가해서 키를 만들자)
			outputKey.set("A," + parser.getYear() + "," + parser.getMonth());
			context.write(outputKey, outputValue);
		} 
		
	}
}
