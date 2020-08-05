package test;

import org.apache.hadoop.io.Text;

public class JKHAirline {
	private int ActualElapsedTime;
	private boolean ActualElapsedTimeable = true;
	private String uniqueCarrier;
	public JKHAirline(Text text) {
		try {
			String [] columns = text.toString().split(","); // csv 파일: ,로 데이터 구분
			uniqueCarrier = columns[8]; // 항공사 코드 : PS
			// NA : 지연이 없다.
			if(!columns[11].equals("NA") && !columns[11].equals("ActualElapsedTime")) {
				ActualElapsedTime = Integer.parseInt(columns[11]);
			} else {
				ActualElapsedTimeable = false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public String getUniqueCarrier() {
		return uniqueCarrier;
	}
	public int getActualElapsedTime() {
		return ActualElapsedTime;
	}
	public void setActualElapsedTime(int actualElapsedTime) {
		ActualElapsedTime = actualElapsedTime;
	}
	public boolean isActualElapsedTimeable() {
		return ActualElapsedTimeable;
	}
	public void setActualElapsedTimeable(boolean actualElapsedTimeable) {
		ActualElapsedTimeable = actualElapsedTimeable;
	}
	
	
}