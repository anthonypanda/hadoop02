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
	 * 重写构造方法
	 */
	public FriendSort() {
		super(Friend.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Friend f1 = (Friend)a;
		Friend f2 = (Friend)b;
		// 名字升序排列
		int c = f1.getFriend1().compareTo(f2.getFriend1());
		if (c == 0) {
			// 亲密度降序排列
			return -Integer.compare(f1.getIntimacy(), f2.getIntimacy());
		}
		return c;
	}

}
