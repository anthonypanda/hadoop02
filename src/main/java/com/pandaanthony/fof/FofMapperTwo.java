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
 * @author lizhuo
 *
 */
public class FofMapperTwo extends Mapper<LongWritable, Text, Friend, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Friend, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// 数据结构：hadoop-cat-2
		String[] strs = StringUtils.split(value.toString(), "-");
		
		// 正反向都要输出
		Friend f1 = new Friend();
		f1.setFriend1(strs[0]);
		f1.setFriend2(strs[1]);
		f1.setIntimacy(Integer.parseInt(strs[2]));
		
		context.write(f1, new IntWritable(f1.getIntimacy()));
		
		Friend f2 = new Friend();
		f2.setFriend1(strs[1]);
		f2.setFriend2(strs[0]);
		f2.setIntimacy(Integer.parseInt(strs[2]));
		
		context.write(f2, new IntWritable(f2.getIntimacy()));
	}

}
