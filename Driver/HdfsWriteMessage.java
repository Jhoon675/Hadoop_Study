package myhadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


// 사용법: HdfsWriteMessage <filename> <message>
// 하둡 안의 configuration을 불러와야 한다.
// 하둡 기본설정
// core-site.xml, hdfs-site.xml, mapred-site.xml ,yarn-site.xml 을 컨피그로 얻는다


public class HdfsWriteMessage {

	public static void main(String[] args) {
//		입력 매개 변수의 개수
//		입력 받아야 하는 매개변수의 개수가 2개인데 매개변수가 2개가 아니면 멈춰라
		if (args.length != 2) {
			System.err.println("Usage: HdfsWriteMessage <filename> <message>");
			System.exit(2);
		}
		
		try {
//		HDFS 파일 시스템 객체 얻어오기
//		Configuration import시 경로 확인 잘 할것
			Configuration conf = new Configuration(); // 현재 서버의 하둡 설정을 읽어온다
			FileSystem hdfs = FileSystem.get(conf); // 그냥 쓰면 error가 나기 때문에 exception 처리를 해준다.
			
			String target = args[0]; // 파일 명
			String message = args[1]; // 저장할 메세지 내용
//			하둡은 특성상 이미 target 파일이 있을때 다시 저장하려고 하면 err가 나기 때문에 삭제해주자 
			
			Path path = new Path(target); // 저장할 파일, 디렉터리의 경로를 담고 있는 객체
			if (hdfs.exists(path)) {
				// 존재하면 삭제
				hdfs.delete(path, false); // delete(path, boolean recursive)
			}
			
//			파일 저장을 위해 출력 스트림을 확보
			FSDataOutputStream os = hdfs.create(path);
			os.writeUTF(message);
			os.close(); // 시스템 자원 사용후에는 반드시 close
			
			System.out.println("메세지가 저장 되었습니다.");
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}
