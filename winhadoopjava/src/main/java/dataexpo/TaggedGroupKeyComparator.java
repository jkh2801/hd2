package dataexpo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// 그룹별로 
public class TaggedGroupKeyComparator extends WritableComparator{
	protected TaggedGroupKeyComparator() {
		super(TaggedKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TaggedKey k1 = (TaggedKey)a;
		TaggedKey k2 = (TaggedKey)b;
		return k1.getCarrierCode().compareTo(k2.getCarrierCode());
	}
	
}
