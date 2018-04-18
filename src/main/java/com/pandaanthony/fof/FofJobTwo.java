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
		// Ĭ�ϼ���src�µ������ļ�������false����src�µ������ļ�
		
		Configuration conf = new Configuration(false);
		// ����hadoop���У�hdfs��resourcemanager���Ƿ������ϵ�
		conf.set("fs.defaultFS", "hdfs://master:9000");
		conf.set("yarn.resourcemanager.hostname", "master");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(FofJobTwo.class);
		
		// ����mapper
		job.setMapperClass(FofMapperTwo.class);
		// ���������key���ͣ�����
		job.setMapOutputKeyClass(Friend.class);
		// ���������value���ͣ�����
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(FriendSort.class);
		job.setGroupingComparatorClass(FriendGroup.class);
		
		// ����reduce
		job.setReducerClass(FofReducerTwo.class);
		FileInputFormat.addInputPath(job, new Path ("/fof/output"));
		
		// �������Path
		Path outputPath = new Path ("/fof/output1");
		FileSystem fs = FileSystem.get(conf);
		// �ж�����ļ��Ƿ���ڣ����ڵĻ�����ɾ������
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
