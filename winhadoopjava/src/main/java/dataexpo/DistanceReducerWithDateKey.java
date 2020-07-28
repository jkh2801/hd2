package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DistanceReducerWithDateKey extends Reducer<DateKey, LongWritable, DateKey, LongWritable> {
	private LongWritable result = new LongWritable();

	protected void reduce(DateKey key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		for(LongWritable v : values) {
			sum += v.get();
		}
		result.set(sum);
		context.write(key, result);

	}
}