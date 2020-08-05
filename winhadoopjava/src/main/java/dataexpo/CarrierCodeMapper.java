package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CarrierCodeMapper extends Mapper<LongWritable, Text, TaggedKey, Text>{
	TaggedKey outkey = new TaggedKey();
	Text outvalue = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) // 입력 스플릿에서 각 키/값 쌍에대해 한번 호출된다.
			throws IOException, InterruptedException {
		CarrierCode code = new CarrierCode(value.toString()); // AA, AB 등등 항공사 정보
		outkey.setCarrierCode(code.getCarrierCode());
		outkey.setTag(0);
		outvalue.set(code.getCarrierName());
		context.write(outkey, outvalue);
	}
	
	
	
}
