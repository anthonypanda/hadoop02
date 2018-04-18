package com.pandaanthony.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ����shuffle������ݽ��д���
 * ������������������ݣ��������2������¶�
 * 
 * @author lizhuo
 *
 */
public class TQReducer extends Reducer<Weather, IntWritable, Text, NullWritable> {

	/*
	 * ��дreduce����
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Weather text, Iterable<IntWritable> iterable, Reducer<Weather, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		// ֻҪǰ2�����ݣ�shuffle�Ѿ��ź�����
		int flag = 0;
		for (IntWritable i : iterable) {
			flag ++;
			if (flag > 2) {
				break;
			}
			// ���һ���ַ�����ע��������¶�����iterable��
			String msg = text.getYear() + "-" + text.getMonth() + "-" +  text.getDay() + ":" + i.get();
			context.write(new Text(msg), NullWritable.get());
		}
	}

}
