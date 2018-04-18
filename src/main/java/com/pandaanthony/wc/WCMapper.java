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
 * mapper的输入是split后的数据，行数可能会很大所以用LongWritable， 内容就是Text
 * 需要输出的是Text和对应的数字1；这里只做分词不做统计
 * 
 * @author lizhuo
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/* 
	 * 重写map方法
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		String[] strs = StringUtils.split(str, " ");
		System.out.println("----" + str);
		// 按空格分词
		for (String s : strs) {
			context.write(new Text (s), new IntWritable(1));
		}
	}

}
