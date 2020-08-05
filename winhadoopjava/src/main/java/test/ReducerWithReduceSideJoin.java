package test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class ReducerWithReduceSideJoin extends Reducer<TaggedKey, Text, TaggedKey, Text>{
	private Text outvalue = new Text();
	@Override
	protected void reduce(TaggedKey key, Iterable<Text> values, Context context) // 각 키/값 쌍에대해 한번 호출된다.
			throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator(); // 반복자
		Text carrierName = new Text(it.next()); // 항공사의 이름
		while(it.hasNext()) {
			Text record = it.next();
			outvalue = new Text(carrierName.toString() + "\t" + record.toString());
			context.write(key, outvalue);
		}
	}
	
	

}
