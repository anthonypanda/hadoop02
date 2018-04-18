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
 * �������ܶȹ�ϵ��reducer
 * ���ﲻ���value����key�����д���
 * �ṹ���£�����1-����2-���ܶ�
 * @author lizhuo
 *
 */
public class FofReducerOne extends Reducer<Text, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable, Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		
		// ���ܶ�
		int sum = 0;
		// �ж��Ƿ�����Ч�Ĺ�ϵ
		boolean flag = true;
		
		for (IntWritable i : iterable) {
			// ����Ҫ��ֱ�Ӻ��ѽ������ܶȼ���
			if (i.get() == 0) {
				flag = false;
				break;
			}
			// ���Ⱥ���
			sum ++;
		}
		
		if (flag) {
			String msg = text.toString() + "-" +  sum;
			context.write(new Text(msg), NullWritable.get());
		}
	}

}
