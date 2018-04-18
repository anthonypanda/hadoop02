package com.pandaanthony.tq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 创建weather对象存储年月日，及温度
 * 数据结构如下：
 * 1949-10-01 14:21:02	34c
 * 
 * @author lizhuo
 *
 */
public class Weather implements WritableComparable<Weather> {
	
	// 年
	private int year = 0;
	
	// 月
	private int month = 0;
	
	// 日
	private int day = 0;
	
	// 温度
	private int temperature = 0;

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getTemperature() {
		return temperature;
	}

	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

	/* 
	 * 反序列化
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput dataInput) throws IOException {
		this.year = dataInput.readInt();
		this.month = dataInput.readInt();
		this.day = dataInput.readInt();
		this.temperature = dataInput.readInt();
	}

	
	/* 
	 * 序列化
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(year);
		dataOutput.writeInt(month);
		dataOutput.writeInt(day);
		dataOutput.writeInt(temperature);
	}

	/* 
	 * 比较年月和温度就可以了，没必要比较天
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Weather weather) {
		int c1 = Integer.compare(this.year, weather.getYear());
		if (c1 == 0) {
			int c2 = Integer.compare(this.month, weather.getMonth());
			if (c2 == 0) {
				return Integer.compare(this.temperature, weather.getTemperature());
			}
			return c2;
		}
		return c1;
	}

}
