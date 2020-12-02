package myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mappers.ArrDelayCountMapper;
import myhadoop.reducer.DelayCountReducer;

public class ArrDelayCount {

	public static void main(String[] args) throws Exception {
	
		if(args.length != 2) {
			System.err.println("Usage: ARRDelayCount <input> <output>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();

		FileSystem hdfs = FileSystem.get(conf);
		
		Path outpath = new Path(args[1]); // 출력 경로
		if (hdfs.exists(outpath)) {
			// 출력 파일이 있으면 지워라
			hdfs.delete(outpath,true);
		}
//		job 생성
		Job job = Job.getInstance(conf,"ArrDelayCount");
		
//		입출력 경로
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outpath);
		
//		실행
		job.setJarByClass(ArrDelayCount.class);		
//		매퍼
		job.setMapperClass(ArrDelayCountMapper.class);
//		reduce
		job.setReducerClass(DelayCountReducer.class);
		
//		입출력 포멧설정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
//		출력 키 ,값 타입 설정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		
	}
}
