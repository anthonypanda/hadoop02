/**
 * 
 */
package com.pandaanthony.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * ����������ݺܶ࣬��Ҫʵ��partion�������������Ч��
 * @author lizhuo
 *
 */
public class TQPartition extends Partitioner<Weather, IntWritable> {

	public int getPartition(Weather key, IntWritable value, int numReduceTasks) {
		// �򵥵�ʵ�ְ���partition
		return (key.getYear() - 1948) % numReduceTasks;
	}

}
