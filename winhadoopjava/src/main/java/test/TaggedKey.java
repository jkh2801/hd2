package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

// key로 사용하기 위해서는 WritableComparable 인터페이스를 구현해야 한다. (리듀서가 key 순서로 자동으로 들어오기 때문에 정렬이 필요하다.)
// 사용자가 생성한 클래스를 하둡의 key로 사용하기 위해서는 WritableComparable을 통해 리듀서에 들어가기 전에 key로 정렬되어야 한다.
public class TaggedKey implements WritableComparable<TaggedKey>{
	// WritableComparable : 맵리듀스 프로그래밍을 위해 네트워크 통신을 위한 객체
	
	private String carrierCode;
	private Integer tag;
	
	public TaggedKey(String carrierCode, Integer tag) {
		this.carrierCode = carrierCode;
		this.tag = tag;
	}

	public TaggedKey() {
	}

	public String getCarrierCode() {
		return carrierCode;
	}

	public void setCarrierCode(String carrierCode) {
		this.carrierCode = carrierCode;
	}

	public Integer getTag() {
		return tag;
	}

	public void setTag(Integer tag) {
		this.tag = tag;
	}

	public void readFields(DataInput in) throws IOException { // 직렬화된 데이터 값을 해체하여 읽는다.
		carrierCode = WritableUtils.readString(in);
		tag = in.readInt();
	}

	public void write(DataOutput out) throws IOException { // 데이터 값을 직렬화
		WritableUtils.writeString(out, carrierCode);
		out.writeInt(tag);
	}

	public int compareTo(TaggedKey key) { // 정렬
		int result = this.carrierCode.compareTo(key.carrierCode);
		if(result  == 0) this.tag.compareTo(key.tag);
		return result;
	}

	@Override
	public String toString() {
		return  carrierCode + ", " + tag;
	}
	

}
