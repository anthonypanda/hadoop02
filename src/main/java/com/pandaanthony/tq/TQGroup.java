/**
 * 
 */
package com.pandaanthony.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * ��дgroup����������ͬ�����·ŵ�һ��group��
 * �����sortһ��Ҫ��д���췽��
 * @author lizhuo
 *
 */
public class TQGroup extends WritableComparator {

	public TQGroup() {
		super (Weather.class, true);
	}
	
	/* 
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Weather w1 = (Weather) a;
		Weather w2 = (Weather) b;
		
		int c1 = Integer.compare(w1.getYear(), w2.getYear());
		if (c1 == 0) {
			int c2 = Integer.compare(w1.getMonth(), w2.getMonth());
			return c2;
		}
		return c1;
	}

}
