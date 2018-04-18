/**
 * 
 */
package com.pandaanthony.fof;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author lizhuo
 *
 */
public class Friend implements WritableComparable<Friend> {
	
	private String friend1;
	
	private String friend2;
	
	// 亲密度
	private int intimacy;

	public String getFriend1() {
		return friend1;
	}

	public void setFriend1(String friend1) {
		this.friend1 = friend1;
	}

	public String getFriend2() {
		return friend2;
	}

	public void setFriend2(String friend2) {
		this.friend2 = friend2;
	}

	public int getIntimacy() {
		return intimacy;
	}

	public void setIntimacy(int intimacy) {
		this.intimacy = intimacy;
	}

	public void readFields(DataInput input) throws IOException {
		this.friend1 = input.readUTF();
		this.friend2 = input.readUTF();
		this.intimacy = input.readInt();
	}

	public void write(DataOutput output) throws IOException {
		output.writeUTF(friend1);
		output.writeUTF(friend2);
		output.writeInt(intimacy);
	}

	/* 
	 * 名字和亲密度比较
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Friend friend) {
		// 比较名字
		int c = this.friend1.compareTo(friend.getFriend1());
		// 名字相同比较亲密度
		if (c == 0) {
			return Integer.compare(this.intimacy, friend.getIntimacy());
		}
		return c;
	}

}
