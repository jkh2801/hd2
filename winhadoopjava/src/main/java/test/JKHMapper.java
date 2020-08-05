package test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class JKHMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	private final static LongWritable time = new LongWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		JKHAirline al = new JKHAirline(value);
		if (al.isActualElapsedTimeable() && al.getActualElapsedTime() > 0) {
			time.set(al.getActualElapsedTime());
			context.write(new Text(al.getUniqueCarrier()), time);
	}
	}
	

}
