package dataexpo;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DelayCountWithDateKey extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		String [] arg = {"-D", "workType=departure", "D:/ubuntushare/dataexpo/1988.csv",
				"depart-1988"};
		// -D :툴 이름
		//  departure-1988 : 출력 위치값
		int res = ToolRunner.run(new Configuration(), new DelayCountWithDateKey(), arg);
		// Configuration : 환경변수, DelayCount : 객체, arg : 매개변수
		
	}

	public int run(String[] args) throws Exception {
		String [] otherargs = new GenericOptionsParser(getConf(),args).getRemainingArgs();
		System.out.println(Arrays.toString(otherargs)); // [hdfs://localhost:9000/user/hadoop/dataexpo/1988.csv, departure-1988]
		if(otherargs.length != 2) {
			System.err.println("Usage: DelayCountWithDateKey <in> <out>");
			// System.err : 표준 오류 객체, 콘솔 출력
			System.exit(2); // 정상종료로 인식
		}
		Job job = new Job(getConf(), "DelayCountWithDateKey");
		FileInputFormat.addInputPath(job, new Path(otherargs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
		// 기존파일 삭제하기 (하둡 시스템의 파일을 제거한다.)
		FileSystem hdfs = FileSystem.get(getConf());
		if(hdfs.exists(new Path(otherargs[1]))) {
			hdfs.delete(new Path(otherargs[1]), true);
			System.out.println("기존 출력파일 삭제");
		}
		job.setJarByClass(DelayCountWithDateKey.class); // job 클래스 설정
		job.setMapperClass(DelayCountMapperWithDateKey.class); // 맵 클래스 설정
		job.setReducerClass(DelayCountReducerWithDateKey.class); // 리듀서 클래스 설정
		job.setInputFormatClass(TextInputFormat.class); // 원본 데이터의 자료형 설정 : 문자형 데이터
		job.setOutputFormatClass(TextOutputFormat.class); // 결과 데이터의 자료형 설정 : 문자형 데이터
		job.setMapOutputKeyClass(DateKey.class); // key 자료형 저장 : 문자형 데이터
		job.setMapOutputValueClass(IntWritable.class); // value 자료형 저장 : 문자형 데이터
		job.setOutputKeyClass(DateKey.class); // 출력키 유형 설정
		job.setOutputValueClass(IntWritable.class); // 출력키 설정
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, DateKey.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, DateKey.class, IntWritable.class);
		job.waitForCompletion(true); // 작업 실행
		return 0;
	}
}
