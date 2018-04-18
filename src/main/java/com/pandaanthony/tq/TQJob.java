/**
 * 
 */
package com.pandaanthony.tq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.base.Preconditions;

/**
 * @author lizhuo
 *
 */
public class TQJob {

	/**
	 * 统计每个年月温度最高的前2天
	 * 数据结构如下：
	 * 1949-10-01 14:21:02	34c
	 * 
	 * 这里要覆盖NativeIO和YARNRunner
	 * 运行方式有3种：
	 * 1、本地hadoop运行
	 * 2、服务器hadoop本地jar运行
	 * 3、服务器运行jar：在shell种敲入 hadoop jar mr.jar com.pandaanthony.wc.WCJob
	 * 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		System.setProperty("HADOOP_USER_NAME", "root");
		// 默认加载src下的配置文件，设置false不读src下的配置文件
//		Configuration conf = new Configuration(true);
//		conf.set("mapred.jar", "D:\\mr.jar");
		
		Configuration conf = new Configuration(false);
		// 本地hadoop运行，hdfs和resourcemanager还是服务器上的
		conf.set("fs.defaultFS", "hdfs://master:9000");
		conf.set("yarn.resourcemanager.hostname", "master");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(TQJob.class);
		
		// 设置mapper
		job.setMapperClass(TQMapper.class);
		// 设置输出的key类型，文字
		job.setMapOutputKeyClass(Weather.class);
		// 设置输出的value类型，数字
		job.setMapOutputValueClass(IntWritable.class);
		
		// 设置reduce
		job.setReducerClass(TQReducer.class);
		
		// 设置partioner， sort， group
		// 设置reducetask个数
		job.setNumReduceTasks(3);
		job.setPartitionerClass(TQPartition.class);
		job.setSortComparatorClass(TQSort.class);
		job.setGroupingComparatorClass(TQGroup.class);
		
		FileInputFormat.addInputPath(job, new Path ("/tq/input/tq"));
		// 设置输出Path
		Path outputPath = new Path ("/tq/output");
		FileSystem fs = FileSystem.get(conf);
		// 判断输出文件是否存在，存在的话进行删除操作
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		boolean flag = job.waitForCompletion(true);
		if (flag) {
			System.out.println("job completed!");
		}

	}

}
