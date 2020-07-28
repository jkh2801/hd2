package wordcount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

// Hadoop 2 버전
public class WordCountTest2 {
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(WordCountTest2.class);
		// JobConf : Job + Configuration (작업 클래스 지정)
		conf.setJobName("wordcount2"); // job의 이름을 설정
		conf.setOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class); // Mapper 클래스를 지정
		conf.setCombinerClass(Reduce.class); // Combiner : Reducer로 가기 전에 분산 서버 자체적으로 Reducer 작업을 실시할 수 있도록 설정 (Network 환경에서 사용)
		// Mapper와 Reducer 사이에서의 병목현상을 최소화함으로서 메모리 부담을 줄일 수 있다.
		conf.setReducerClass(Reduce.class); // Reducer 클래스를 지정
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("infile/in.txt"), new Path("infile/in2.txt")); // 여러개의 파일도 처리가 가능하다.
		FileOutputFormat.setOutputPath(conf, new Path("outfile/wordcount"));
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path("outfile/wordcount"))) {
			hdfs.delete(new Path("outfile/wordcount"), true);
			System.out.println("기존 출력파일 삭제");
		}
		JobClient.runJob(conf);
	}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			while(itr.hasMoreElements()) {
				word.set(itr.nextToken());
				output.collect(word, one);
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while(value.hasNext()) sum += value.next().get();
			output.collect(key, new IntWritable(sum));
		}
		
		
	}
}
