package com.pandaanthony.fof;

/**
 * ���ɺ��ѹ�ϵ
 * a-b��b-a��ͬһ�ֹ�ϵ
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
