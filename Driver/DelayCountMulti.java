package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mappers.DelayCountMapperMulti;
import myhadoop.reducer.DelayCountReducerMulti;

public class DelayCountMulti {
	
	public static void main(String[] args) throws Exception{
	
	// 매개변수 확인	
		if (args.length != 2) {
			System.err.println("Usage: DelayCountMulti <input> <output>");
			System.exit(2);
		}
		
		// 설정 정보
		Configuration conf = new Configuration();
		
		// job설정
		Job job = Job.getInstance(conf, "DelayCountMulti");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		// 실행파일 설정
		job.setJarByClass(DelayCountMulti.class);
		job.setMapperClass(DelayCountMapperMulti.class);
		job.setReducerClass(DelayCountReducerMulti.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 명명된 출력 경로를 등록
		MultipleOutputs.addNamedOutput(job,
				"departure", // 출력 경로의 이름,
				TextOutputFormat.class,
				Text.class,
				IntWritable.class);
		
		MultipleOutputs.addNamedOutput(job,
				"arrival",
				TextOutputFormat.class,
				Text.class,
				IntWritable.class);
		
		job.waitForCompletion(true);
	}
	
}
