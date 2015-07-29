package com.songguoliang.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * HBase与WordCount的结合使用Demo
 * @date 2015-07-27 11:21:48
 * @author sgl
 */
public class WordCountHBase {
	/**
	 * Map
	 * @date 2015-07-27 11:24:04
	 * @author sgl
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable one=new IntWritable(1);
		/*
		 * 重写map方法
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 * @date 2015-07-27 11:29:48
		 * @author sgl
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			//将输入的每行内容以空格分开
			String words[]=value.toString().trim().split(" ");
			for(String word:words){
				context.write(new Text(word), one);
			}
		}
	}
	/**
	 * Reduce
	 * @date 2015-07-27 11:36:03
	 * @author sgl
	 * @version $Id: WordCountHBase.java, v 0.1 2015-07-27 11:36:03 sgl Exp $
	 */
	public static class Reduce extends TableReducer<Text, IntWritable, NullWritable>{
		/*
		 * 重写reduce方法
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 * @date 2015-07-27 11:36:12
		 * @author sgl
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, NullWritable, Writable>.Context context) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable value:values){
				sum+=value.get();
			}
			//Put实例化，每一个单词存一行
			Put put=new Put(Bytes.toBytes(key.toString()));
			//列族为content,列修饰符为count,列值为数量
			put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
			context.write(NullWritable.get(), put);
		}
		
	}
	/**
	 * 在HBase中创建表
	 * @date 2015-07-27 11:50:42
	 * @author sgl
	 * @param tableName 表名
	 * @throws IOException
	 */
	public static void createHBaseTable(String tableName) throws IOException{
		HTableDescriptor tableDescriptor=new HTableDescriptor(tableName);
		HColumnDescriptor columnDescriptor=new HColumnDescriptor("content");
		tableDescriptor.addFamily(columnDescriptor);
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "sdw1,sdw2");
		HBaseAdmin admin=new HBaseAdmin(conf);
		if(admin.tableExists(tableName)){
			System.out.println("表已存在，正在尝试重新创建表！");
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		System.out.println("创建新表："+tableName);
		admin.createTable(tableDescriptor);
	}
	
	public static void main(String[] args) {
		try {
			String tableName="wordcount";
			createHBaseTable(tableName);
			
			Configuration conf=new Configuration();
			conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
			conf.set("hbase.zookeeper.quorum", "sdw1,sdw2");
			String input=args[0];
			Job job=new Job(conf, "WordCount table with "+input);
			job.setJarByClass(WordCountHBase.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TableOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path(input));
			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}

