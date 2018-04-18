/**
 * 
 */
package com.pandaanthony.tq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * mapper的输入是split后的数据，行数可能会很大所以用LongWritable， 内容就是Text
 * 需要输出的是年月日及对应的温度
 * 数据结构如下：
 * 1949-10-01 14:21:02	34c
 * @author lizhuo
 *
 */
public class TQMapper extends Mapper<LongWritable, Text, Weather, IntWritable> {

	/* 
	 * 重写map方法
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Weather, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// 这里的数据格式是Tab键分割的，所以用制表符\t
		String[] strs = StringUtils.split(value.toString(), "\t");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(sdf.parse(strs[0]));
			// 构建Weather对象
			Weather weather = new Weather ();
			weather.setYear(cal.get(Calendar.YEAR));
			// 月份需要加1
			weather.setMonth(cal.get(Calendar.MONTH) + 1);
			weather.setDay(cal.get(Calendar.DAY_OF_MONTH));
			
			// 获取温度
			int temperature = Integer.parseInt(strs[1].substring(0, strs[1].lastIndexOf("c")));
			weather.setTemperature(temperature);
			System.out.println (weather.getYear());
			context.write(weather, new IntWritable(temperature));
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
