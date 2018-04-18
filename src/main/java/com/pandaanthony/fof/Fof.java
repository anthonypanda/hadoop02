package com.pandaanthony.fof;

/**
 * 生成好友关系
 * a-b和b-a是同一种关系
 * @author lizhuo
 *
 */
public class Fof {

	public String format(String friend1, String friend2) {
		int c1 = friend1.compareTo(friend2);
		
		if (c1 < 0) {
			return friend2 + "-" +  friend1;
		}
		
		return friend1 + "-" + friend2;
	}

}
