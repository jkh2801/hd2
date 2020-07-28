package dataexpo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// 월별 운항 거리 출력
public class DistanceWithDateKey{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "DistanceWithDateKey"); // 작업 설정
		FileInputFormat.addInputPath(job, new Path("D:/ubuntushare/dataexpo/1988.csv"));
		FileOutputFormat.setOutputPath(job, new Path("outfile/distance"));
		// 기존파일 삭제하기 (하둡 시스템의 파일을 제거하기)
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path("outfile/distance"))) {
			hdfs.delete(new Path("outfile/distance"), true);
			System.out.println("기존 출력파일 삭제");
		}
		job.setJarByClass(DistanceWithDateKey.class); // job 클래스 설정
		job.setMapperClass(DistanceMapperWithDateKey.class); // 맵 클래스 설정
		job.setReducerClass(DistanceReducerWithDateKey.class); // 리듀서 클래스 설정
		job.setMapOutputKeyClass(DateKey.class); // key 자료형 저장 : 문자형 데이터
		job.setMapOutputValueClass(LongWritable.class); // value 자료형 저장 : 문자형 데이터
		job.setInputFormatClass(TextInputFormat.class); // 원본 데이터의 자료형 설정 : 문자형 데이터
		job.setOutputFormatClass(TextOutputFormat.class); // 결과 데이터의 자료형 설정 : 문자형 데이터
		job.setOutputKeyClass(DateKey.class); // 출력키 유형 설정
		job.setOutputValueClass(LongWritable.class); // 출력키 설정
		job.waitForCompletion(true); // 작업 실행
	}

}
