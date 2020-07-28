package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistanceMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, LongWritable> {
	private final static LongWritable distance = new LongWritable();
	private DateKey outputKey = new DateKey();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Airline al = new Airline(value);
		if (al.isDistanceAvailable() && al.getDistance() > 0) {
				outputKey.setYear(""+al.getYear());
				outputKey.setMonth(al.getMonth());
				distance.set(al.getDistance());
				context.write(outputKey, distance);
		}
	}
}
