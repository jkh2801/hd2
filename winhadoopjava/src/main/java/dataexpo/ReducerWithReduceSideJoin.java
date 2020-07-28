package dataexpo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWithReduceSideJoin extends Reducer<TaggedKey, Text, Text, Text>{
	private Text outkey = new Text();
	private Text outvalue = new Text();
	@Override
	protected void reduce(TaggedKey key, Iterable<Text> values, Context context) // 각 키/값 쌍에대해 한번 호출된다.
			throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		Text carrierName = new Text(it.next());
		while(it.hasNext()) {
			Text record = it.next();
			outkey.set(key.getCarrierCode());
			outvalue = new Text(carrierName.toString() + "\t" + record.toString());
			context.write(outkey, outvalue);
		}
	}
	
	

}
