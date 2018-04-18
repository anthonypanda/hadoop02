package com.pandaanthony.wc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 接受shuffle后的数据进行处理
 * 输入的数据是多个相同文字和对应的数字1，输出需要将数字相加
 * 
 * @author lizhuo
 *
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/*
	 * 重写reduce方法
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : iterable) {
			sum += i.get();
		}
		System.out.println("----");
		/*Iterator<IntWritable> iterator = iterable.iterator();
		while (iterator.hasNext()) {
			sum += iterator.next().get();
		}*/
		context.write(text, new IntWritable(sum));
	}

}
