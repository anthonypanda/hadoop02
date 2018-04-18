package com.pandaanthony.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//统计df：词在多少个微博中出现过
public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// 获取当前 mapper task的数据片�?(split)
		FileSplit fs = (FileSplit) context.getInputSplit();

		if (!fs.getPath().getName().contains("part-r-00003")) {

			String[] v = value.toString().trim().split("\t");
			if (v.length >= 2) {
				String[] ss = v[0].split("_");
				if (ss.length >= 2) {
					String w = ss[0];
					context.write(new Text(w), new IntWritable(1));
				}
			} else {
				System.out.println(value.toString() + "-------------");
			}
		}
	}
}
