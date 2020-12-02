package myhadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import myhdoop.suppot.AirlinePerformanceParser;

// 입력 키 : LingWritable(행번호), 값: (Text) csv 한행 yaer, month,....
// 출력 키 : 년, 월 -> (Text), 값: IntWritable 1
public class DeptDelayCountMapper 
			extends Mapper<LongWritable, // 임의로 1번
							Text, // 임의로 2번
							Text, // 임의로 3번이라 설정
							IntWritable>{ // 임의로 4번
//		맵의 출력값
//		항상 1이니 미리 써준다.
//		4번
		private final static IntWritable outputValue = new IntWritable(1);
//		맵 출력 키를 설정하기 위한 객체
		private Text outputkey = new Text();
		
		@Override
		protected void map(LongWritable key, // 행 번호
				Text value, // 이 경우에는, csv 한행(, 로 구분된 문자열)
				Context context) // MapReduce의 전체 문맥
				throws IOException, InterruptedException {
//			 이 경우에 첫번째 행은 header이고 header 는 건너뛰자
			if(key.get() == 0 && value.toString().contains("YEAR")) {
				// 헤더 라인이므로 중단 -> 리듀서로 전달되지 않음
				return;
			}
			
//			 csv 분석
			AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
			
//			 만약, department_delay > 0보다 크면 출력 (key:년 , value: 월) -> 출력
			if(parser.getDepartureDelayTime() > 0 ) {
//				 출발 지연 상태
//				 출발 키 설정
				outputkey.set(parser.getYear() + "," + parser.getMonth()); // 예시 2010,1
				context.write(outputkey, outputValue);
			}

		}
}




