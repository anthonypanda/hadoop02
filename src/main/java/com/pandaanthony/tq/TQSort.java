/**
 * 
 */
package com.pandaanthony.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * ��дshuffleʱ��sort����
 * ����ע��Ҫ��д���췽��
 * @author lizhuo
 *
 */
public class TQSort extends WritableComparator {

	public TQSort() {
		super(Weather.class, true);
	}

	/* 
	 * �����ǰ����������ţ��¶Ƚ�����
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Weather w1 = (Weather) a;
		Weather w2 = (Weather) b;
		
		int c1 = Integer.compare(w1.getYear(), w2.getYear());
		if (c1 == 0) {
			int c2 = Integer.compare(w1.getMonth(), w2.getMonth());
			if (c2 == 0) {
				return - Integer.compare(w1.getTemperature(), w2.getTemperature());
			}
			return c2;
		}
		return c1;
	}

}
