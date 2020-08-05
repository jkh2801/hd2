package test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// 그룹을 설정하기 위한 partitioner 클래스
// 평소엔 자동으로 hashcode를 이용하여 group화를 한다. --> 복합키인 경우 의미가 없다.(partitioner 클래스를 이용하여 그룹화 한다.)
public class TaggedGroupKeyPartitioner extends Partitioner<TaggedKey, Text>{

	@Override
	public int getPartition(TaggedKey key, Text value, int numPartitions) { // 항공사 코드의 해시값으로 파티션 계산
		int hash = key.getCarrierCode().hashCode(); // 항공사 별로 해시값을 가져온다.
		int partition = hash % numPartitions; // partitioner을 생성
		return partition;
	}
	
}
