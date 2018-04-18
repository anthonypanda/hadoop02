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
	 * ͳ��ÿ�������¶���ߵ�ǰ2��
	 * ���ݽṹ���£�
	 * 1949-10-01 14:21:02	34c
	 * 
	 * ����Ҫ����NativeIO��YARNRunner
	 * ���з�ʽ��3�֣�
	 * 1������hadoop����
	 * 2��������hadoop����jar����
	 * 3������������jar����shell������ hadoop jar mr.jar com.pandaanthony.wc.WCJob
	 * 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		System.setProperty("HADOOP_USER_NAME", "root");
		// Ĭ�ϼ���src�µ������ļ�������false����src�µ������ļ�
//		Configuration conf = new Configuration(true);
//		conf.set("mapred.jar", "D:\\mr.jar");
		
		Configuration conf = new Configuration(false);
		// ����hadoop���У�hdfs��resourcemanager���Ƿ������ϵ�
		conf.set("fs.defaultFS", "hdfs://master:9000");
		conf.set("yarn.resourcemanager.hostname", "master");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(TQJob.class);
		
		// ����mapper
		job.setMapperClass(TQMapper.class);
		// ���������key���ͣ�����
		job.setMapOutputKeyClass(Weather.class);
		// ���������value���ͣ�����
		job.setMapOutputValueClass(IntWritable.class);
		
		// ����reduce
		job.setReducerClass(TQReducer.class);
		
		// ����partioner�� sort�� group
		// ����reducetask����
		job.setNumReduceTasks(3);
		job.setPartitionerClass(TQPartition.class);
		job.setSortComparatorClass(TQSort.class);
		job.setGroupingComparatorClass(TQGroup.class);
		
		FileInputFormat.addInputPath(job, new Path ("/tq/input/tq"));
		// �������Path
		Path outputPath = new Path ("/tq/output");
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
