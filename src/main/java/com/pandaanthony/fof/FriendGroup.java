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
public class FriendGroup extends WritableComparator {

	public FriendGroup() {
		super(Friend.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Friend f1 = (Friend)a;
		Friend f2 = (Friend)b;
		// Ãû×ÖÉıĞòÅÅÁĞ
		int c = f1.getFriend1().compareTo(f2.getFriend1());
		return c;
	}

}
