package test;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedGroupComparator extends WritableComparator{
	protected TaggedGroupComparator() {
		super(TaggedKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TaggedKey k1 = (TaggedKey)a;
		TaggedKey k2 = (TaggedKey)b;
		return k2.getCarrierCode().compareTo(k2.getCarrierCode());
	}
	
	
}
