package com.pandaanthony.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 把同现矩阵和得分矩阵相乘<br>
 * 利用MR原语特征，按商品分组<br>
 * 这样相同商品的同现列表和�?有用户对该商品的评分进到�?个reduce�?<br>
 * 
 * 
 * @author root
 */
public class Step4 {

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("step4");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step4_Mapper.class);
			job.setReducerClass(Step4_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job,
					new Path[] { new Path(paths.get("Step4Input1")),
							new Path(paths.get("Step4Input2")) });
			Path outpath = new Path(paths.get("Step4Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag;// A同现矩阵 or B得分矩阵

		// 每个maptask，初始化时调用一�?
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();// 判断读的数据�?

			System.out.println(flag + "**********************");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());

			if (flag.equals("step3")) {// 同现矩阵
				// 样本:  i100:i181	1
				//       i100:i184	2
				String[] v1 = tokens[0].split(":");
				String itemID1 = v1[0];
				String itemID2 = v1[1];
				String num = tokens[1];

				Text k = new Text(itemID1);// 以前�?个物品为key 比如i100
				Text v = new Text("A:" + itemID2 + "," + num);// A:i109,1
				// 样本:  i100	A:i181,1
				context.write(k, v);

			} else if (flag.equals("step2")) {// 用户对物品喜爱得分矩�?
				// 样本:  u24  i64:1,i218:1,i185:1,
				String userID = tokens[0];
				for (int i = 1; i < tokens.length; i++) {
					String[] vector = tokens[i].split(":");
					String itemID = vector[0];// 物品id
					String pref = vector[1];// 喜爱分数

					Text k = new Text(itemID); // 以物品为key 比如：i100
					Text v = new Text("B:" + userID + "," + pref); // B:u401,2
					// 样本:  i64	B:u24,1
					context.write(k, v);
				}
			}
		}
	}

	static class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// A同现矩阵 or B得分矩阵
			// 某一个物品，针对它和其他�?有物品的同现次数，都在mapA集合�?
			Map<String, Integer> mapA = new HashMap<String, Integer>();
			//和该物品（key中的itemID）同现的其他物品的同现集�?//
			//其他物品ID为map的key，同现数字为�?
			
			Map<String, Integer> mapB = new HashMap<String, Integer>();
			//该物品（key中的itemID），�?有用户的推荐权重分数

			for (Text line : values) {
				String val = line.toString();
				if (val.startsWith("A:")) {// 表示物品同现数字
					String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
					try {
						mapA.put(kv[0], Integer.parseInt(kv[1]));
					} catch (Exception e) {
						e.printStackTrace();
					}

				} else if (val.startsWith("B:")) {
					String[] kv = Pattern.compile("[\t,]").split(
							val.substring(2));
					try {
						mapB.put(kv[0], Integer.parseInt(kv[1]));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			
			double result = 0;
			//同现矩阵A
			Iterator<String> iter = mapA.keySet().iterator();
			//MR原语特征，这里只有一种商品的同现列表
			while (iter.hasNext()) {
				String mapk = iter.next();// itemID

				int num = mapA.get(mapk).intValue();
				Iterator<String> iterb = mapB.keySet().iterator();
				//MR原语特征，这里是�?有用户的同一商品的评分，迭代�?
				while (iterb.hasNext()) {//迭代用户�?
					String mapkb = iterb.next();// userID
					int pref = mapB.get(mapkb).intValue();
					//注意这里的计算�?�维理解�?
						//针对A商品
						//使用用户对A商品的分�?
						//逐一乘以与A商品有同现的商品的次�?
						//但是计算推荐向量的时候需要的是A商品同现的商品，用同现次数乘以各自的分�??
					result = num * pref;// 矩阵乘法相乘计算

//					Text k = new Text(mapkb);
//					Text v = new Text(mapk + "," + result);
//				//  结果样本:  u2723    	i9,8.0
//					context.write(k, v);
					
					Text k = new Text(mapkb+","+mapk);
					Text v = new Text( key.toString() + "," + result);
					//key:101
					//  结果样本:   u3,101   101,4.0   *
					//  结果样本:   u3,102   101,4.0   
					//  结果样本:   u3,103   101,4.0
					//key:102
					//  结果样本:   u3,101   102,4.0   *
					//  结果样本:   u3,102   102,4.0   
					//  结果样本:   u3,103   102,4.0
					
					context.write(k, v);
				}
			}
		}
	}
}