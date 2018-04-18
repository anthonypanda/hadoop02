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
 * �Ƽ����ѣ������ȼ���ߵ��Ƽ���
 * 
 * ���ݽṹ: ��һ��Ϊ��ǰ�û���������ÿո�ֿ���Ϊ�����
 * tom hello hadoop cat
 * 
 * ������Ҫ������һ�����Ⱥ��ѣ����Ұ������ܶȽ����reduce�����
 * Ȼ���ٸ��ݽṹ�õ���Ӧ���û��Ƽ��ĺ���
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
		// Ĭ�ϼ���src�µ������ļ�������false����src�µ������ļ�
		
		Configuration conf = new Configuration(false);
		// ����hadoop���У�hdfs��resourcemanager���Ƿ������ϵ�
		conf.set("fs.defaultFS", "hdfs://master:9000");
		conf.set("yarn.resourcemanager.hostname", "master");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(FofJobOne.class);
		
		// ����mapper
		job.setMapperClass(FofMapperOne.class);
		// ���������key���ͣ�����
		job.setMapOutputKeyClass(Text.class);
		// ���������value���ͣ�����
		job.setMapOutputValueClass(IntWritable.class);
		
		// ����reduce
		job.setReducerClass(FofReducerOne.class);
		FileInputFormat.addInputPath(job, new Path ("/fof/input/fof"));
		
		// �������Path
		Path outputPath = new Path ("/fof/output");
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
