package myhadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 입력: 1987, 1 -> 키, 1 -> 값
// 출력: 1987, 1 -> 키, 입력 값의 집계 결과 -> 값
// 집계 리듀서는 공통으로 사용할 수 있다.

public class DelayCountReducer 
	extends Reducer<Text, // 입력 타입
					IntWritable, // 리듀서 입력 값의 타입
					Text, // 리듀스 출력 키의 타입, 출력 되는 형태
					IntWritable>{ // 리듀서 출력 값의 타입
//	리듀서 출력 값의 객체 
//	집계 값이 저장되는 객체.
	private IntWritable result = new IntWritable();
	@Override
	protected void reduce(Text key, // 키에서 사용될 데이터 타입 
			Iterable<IntWritable> values, // Iterable: 여러개가 넘어온다. 입력 값의 순회 객체
			Context context) throws InterruptedException {
//		values에 있는 모든 값을 합산 -> 결과로 출력.
//		입력 키와 출력 키가 같으니까 재활용
		try {
			int sum = 0;
	//		집계
			for (IntWritable value: values) {
				sum += value.get();			
			}
	//	 출력 객체를 세팅
			result.set(sum);
	//	출력
			context.write(key, result);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
