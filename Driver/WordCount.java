package myhadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import myhadoop.mappers.WordCountMapper;
import myhadoop.reducer.WordCountReducer;

// 불꽃처럼 꺼지지 않는 이마음으로 그동안에 넌 얼마나 힘이 들었니~ 천년이가도

// MapReduce Driver 클래스
/*
 * 1. 잡 객체를 생성
 * 2. 잡 객체에 맵 리듀스 실행 정보 설정
 * 3. 맵 리듀스 작업 수행
 */
public class WordCount {

	public static void main(String[] args) throws Exception{
		// 파라미터 체크, 부족하면 종료
		if (args.length != 2) {
			System.err.println("Usage: WordCount <input> <output>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCount");
	
//		실행을 위한 클래스 등록
		job.setJarByClass(WordCount.class); // 드라이버 클래스 명시
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
//		입출력 포맷 등록
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
//		출력의 키와 값의 타입을 등록: (단어, 합)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
//		입출력 경로를 등록
		FileInputFormat.addInputPath(job, new Path(args[0])); // 입력 소스 경로
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // 출력 타겟 경로
		
//		수행
		job.waitForCompletion(true);
		

	}
}
// job: 전체를 의미한다.