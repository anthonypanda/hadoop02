/**
 * 
 */
package com.pandaanthony.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 这里假设数据很多，需要实现partion方法，提高运行效率
 * @author lizhuo
 *
 */
public class TQPartition extends Partitioner<Weather, IntWritable> {

	public int getPartition(Weather key, IntWritable value, int numReduceTasks) {
		// 简单的实现按年partition
		return (key.getYear() - 1948) % numReduceTasks;
	}

}
