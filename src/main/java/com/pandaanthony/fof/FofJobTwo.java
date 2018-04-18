/**
 * 
 */
package com.pandaanthony.fof;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author lizhuo
 *
 */
public class FofJobTwo {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		// 默认加载src下的配置文件，设置false不读src下的配置文件
		
		Configuration conf = new Configuration(false);
		// 本地hadoop运行，hdfs和resourcemanager还是服务器上的
		conf.set("fs.defaultFS", "hdfs://master:9000");
		conf.set("yarn.resourcemanager.hostname", "master");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(FofJobTwo.class);
		
		// 设置mapper
		job.setMapperClass(FofMapperTwo.class);
		// 设置输出的key类型，文字
		job.setMapOutputKeyClass(Friend.class);
		// 设置输出的value类型，数字
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(FriendSort.class);
		job.setGroupingComparatorClass(FriendGroup.class);
		
		// 设置reduce
		job.setReducerClass(FofReducerTwo.class);
		FileInputFormat.addInputPath(job, new Path ("/fof/output"));
		
		// 设置输出Path
		Path outputPath = new Path ("/fof/output1");
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
