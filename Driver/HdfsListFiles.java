//package myhadoop;
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.LocatedFileStatus;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.RemoteIterator;
//
//// 사용법: HdfsListFiles 목적지
//public class HdfsListFiles {
//
//	public static void main(String[] args) {
////		목적지 매개변수가 없으면 err
//		if (args.length != 1) {
//			System.err.println("Usage: HdfsListFiles <dir>");
//			System.exit(2);
//		}		
//		try {
////		설정 불러와서 파일 시스템 얻기
//			Configuration conf = new Configuration();
////		1번쨰 매개변수에서 목적지 dir 확보
//			String dir = args[0];
//			FileSystem hdfs = FileSystem.get(conf);
//			Path path = new Path(dir);
//			
//			RemoteIterator<LocatedFileStatus> iter = hdfs.listFiles(path, true);
//			
////			 Iterator를 순회하면서 내부 객체를 추출
//			while(iter.hasNext()) { // 뒤에 내용이 더 있으면 루프를 돌릴 수 있다.
//				LocatedFileStatus 
//				System.out.printf("%s, %s%n", status.isDirectory() ? "Directory":"File",status.getPath());
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//}
