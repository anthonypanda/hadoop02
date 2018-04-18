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

import com.pandaanthony.wc.WCJob;
import com.pandaanthony.wc.WCMapper;
import com.pandaanthony.wc.WCReducer;

/**
 * 推荐好友，按优先级最高的推荐。
 * 
 * 数据结构: 第一个为当前用户，后面的用空格分开的为其好友
 * tom hello hadoop cat
 * 
 * 这里需要先生成一个二度好友，并且按照亲密度降序的reduce结果，
 * 然后再根据结构得到对应的用户推荐的好友
 * 
 * @author lizhuo
 *
 */
public class FofJobOne {

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
		job.setJarByClass(FofJobOne.class);
		
		// 设置mapper
		job.setMapperClass(FofMapperOne.class);
		// 设置输出的key类型，文字
		job.setMapOutputKeyClass(Text.class);
		// 设置输出的value类型，数字
		job.setMapOutputValueClass(IntWritable.class);
		
		// 设置reduce
		job.setReducerClass(FofReducerOne.class);
		FileInputFormat.addInputPath(job, new Path ("/fof/input/fof"));
		
		// 设置输出Path
		Path outputPath = new Path ("/fof/output");
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
