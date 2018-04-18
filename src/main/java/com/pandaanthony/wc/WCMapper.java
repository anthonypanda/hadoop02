/**
 * 
 */
package com.pandaanthony.wc;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * mapper��������split������ݣ��������ܻ�ܴ�������LongWritable�� ���ݾ���Text
 * ��Ҫ�������Text�Ͷ�Ӧ������1������ֻ���ִʲ���ͳ��
 * 
 * @author lizhuo
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/* 
	 * ��дmap����
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		String[] strs = StringUtils.split(str, " ");
		System.out.println("----" + str);
		// ���ո�ִ�
		for (String s : strs) {
			context.write(new Text (s), new IntWritable(1));
		}
	}

}
