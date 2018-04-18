/**
 * 
 */
package com.pandaanthony.fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author lizhuo
 *
 */
public class FriendSort extends WritableComparator {

	
	/**
	 * ��д���췽��
	 */
	public FriendSort() {
		super(Friend.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Friend f1 = (Friend)a;
		Friend f2 = (Friend)b;
		// ������������
		int c = f1.getFriend1().compareTo(f2.getFriend1());
		if (c == 0) {
			// ���ܶȽ�������
			return -Integer.compare(f1.getIntimacy(), f2.getIntimacy());
		}
		return c;
	}

}
