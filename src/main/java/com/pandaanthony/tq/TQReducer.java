package com.pandaanthony.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 接受shuffle后的数据进行处理
 * 输入的是天气对象数据，输出的是2个最高温度
 * 
 * @author lizhuo
 *
 */
public class TQReducer extends Reducer<Weather, IntWritable, Text, NullWritable> {

	/*
	 * 重写reduce方法
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Weather text, Iterable<IntWritable> iterable, Reducer<Weather, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		// 只要前2条数据，shuffle已经排好序了
		int flag = 0;
		for (IntWritable i : iterable) {
			flag ++;
			if (flag > 2) {
				break;
			}
			// 输出一个字符串；注意这里的温度是在iterable中
			String msg = text.getYear() + "-" + text.getMonth() + "-" +  text.getDay() + ":" + i.get();
			context.write(new Text(msg), NullWritable.get());
		}
	}

}
