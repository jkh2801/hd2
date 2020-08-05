package test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import dataexpo.DateKey;
import dataexpo.DistanceMapperWithDateKey;
import dataexpo.DistanceReducerWithDateKey;
import dataexpo.DistanceWithDateKey;

public class JKHMain {
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "JKHMain"); // 작업 설정
		FileInputFormat.addInputPath(job, new Path("D:/ubuntushare/dataexpo/1993.csv"));
		FileOutputFormat.setOutputPath(job, new Path("outfile/ActualElapsedTime"));
		// 기존파일 삭제하기 (하둡 시스템의 파일을 제거하기)
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path("outfile/ActualElapsedTime"))) {
			hdfs.delete(new Path("outfile/ActualElapsedTime"), true);
			System.out.println("기존 출력파일 삭제");
		}
		job.setJarByClass(JKHMain.class); // job 클래스 설정
		job.setMapperClass(JKHMapper.class); // 맵 클래스 설정
		job.setReducerClass(JKHReducer.class); // 리듀서 클래스 설정
		job.setMapOutputKeyClass(Text.class); // key 자료형 저장 : 문자형 데이터
		job.setMapOutputValueClass(LongWritable.class); // value 자료형 저장 : 문자형 데이터
		job.setInputFormatClass(TextInputFormat.class); // 원본 데이터의 자료형 설정 : 문자형 데이터
		job.setOutputFormatClass(TextOutputFormat.class); // 결과 데이터의 자료형 설정 : 문자형 데이터
		job.setOutputKeyClass(Text.class); // 출력키 유형 설정
		job.setOutputValueClass(LongWritable.class); // 출력키 설정
		job.waitForCompletion(true); // 작업 실행
	}
}
