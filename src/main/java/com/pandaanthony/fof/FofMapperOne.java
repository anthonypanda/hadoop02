/**
 * 
 */
package com.pandaanthony.fof;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 生成用户及亲密度关系的mapper
 * 
 * 数据结构: 第一个为当前用户，后面的用空格分开的为其好友
 * tom hello hadoop cat
 * 
 * @author lizhuo
 *
 */
public class FofMapperOne extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String[] strs = StringUtils.split(value.toString(), " ");
		
		Fof fof = new Fof ();
		for (int i = 0; i < strs.length; i ++) {
			// 已知好友关系
			String s1 = fof.format(strs[0], strs[i]);
			context.write(new Text (s1), new IntWritable(0));
			
			for (int j = i + 1; j < strs.length; j ++) {
				// 二度好友关系
				String s2 = fof.format(strs[i], strs[j]);
				context.write(new Text (s2), new IntWritable(1));
			}
		}
	}

}
