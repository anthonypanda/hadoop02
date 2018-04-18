/**
 * 
 */
package com.pandaanthony.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 生成亲密度关系的reducer
 * 这里不输出value，用key来进行处理
 * 结构如下：好友1-好友2-亲密度
 * @author lizhuo
 *
 */
public class FofReducerOne extends Reducer<Text, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable, Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		
		// 亲密度
		int sum = 0;
		// 判断是否是有效的关系
		boolean flag = true;
		
		for (IntWritable i : iterable) {
			// 不需要对直接好友进行亲密度计算
			if (i.get() == 0) {
				flag = false;
				break;
			}
			// 二度好友
			sum ++;
		}
		
		if (flag) {
			String msg = text.toString() + "-" +  sum;
			context.write(new Text(msg), NullWritable.get());
		}
	}

}
