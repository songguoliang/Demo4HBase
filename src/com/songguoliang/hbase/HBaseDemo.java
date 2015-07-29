/**
 * Copyright (c) 2006-2015 Dealer Online Team. 2006-2015,All Rights Reserved.
 * This software is published under the Dealer Online Solution Team.
 * License version 1.0, a copy of which has been included with this
 * distribution in the LICENSE.txt file.
 *
 * @File name:  HBaseDemo.java
 * @Create on:  2015-07-20 11:20:41
 * @Author   :  sgl
 *
 * @ChangeList
 * ---------------------------------------------------
 * NO      Date               		Editor    	ChangeReasons
 * 1       2015-07-20 11:20:41    	sgl	       	Create 
 *
 */
package com.songguoliang.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase API操作Demo
 * @date 2015-07-20 11:25:47
 * @author sgl
 */
public class HBaseDemo {
	private static Configuration conf=null;
	static{
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "sdw1,sdw2");
	}
	/**
	 * 创建一个表
	 * @date 2015-07-20 11:27:59
	 * @author sgl
	 * @param tableName 表名
	 * @param familys 列族
	 * @throws IOException 
	 */
	public static void createTable(String tableName,String[]familys) throws IOException{
		HBaseAdmin admin=null;
		try {
			admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				System.out.println("表已存在!");
			} else {
				HTableDescriptor descriptor = new HTableDescriptor(tableName);
				for (int i = 0; i < familys.length; i++) {
					descriptor.addFamily(new HColumnDescriptor(familys[i]));
				}
				admin.createTable(descriptor);
				System.out.println("创建表 " + tableName + " 成功!");
			}
		} finally{
			if(admin!=null){
				admin.close();
			}
		}
	}
	/**
	 * 删除表
	 * @date 2015-07-20 19:30:37
	 * @author sgl
	 * @param tableName 表名
	 * @throws IOException 
	 */
	public static void deleteTable(String tableName) throws IOException{
		HBaseAdmin admin =new HBaseAdmin(conf);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		admin.close();
		System.out.println("删除表"+tableName+"成功！");
	}
	/**
	 * 添加记录
	 * @date 2015-07-20 19:41:34
	 * @author sgl
	 * @param tableName 表名
	 * @param rowKey 行健
	 * @param family 列族
	 * @param qualifier 列限定符
	 * @param value 值
	 * @throws IOException 
	 */
	public static void addRecourd(String tableName,String rowKey,String family,String qualifier,String value) throws IOException{
		HTable table=new HTable(conf, tableName);
		Put put=new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
		table.put(put);
		System.out.println("向表"+tableName+"插入记录"+rowKey+"成功！");
		table.close();
	}
	/**
	 * 获取一条记录
	 * @date 2015-07-20 20:03:53
	 * @author sgl
	 * @param tableName 表名
	 * @param rowKey 行健
	 * @throws IOException
	 */
	public static void getOneRecord(String tableName,String rowKey) throws IOException{
		HTable table=new HTable(conf,tableName);
		Get get=new Get(rowKey.getBytes());
		Result result=table.get(get);
		for(KeyValue kv:result.raw()){
			System.out.print(new String(kv.getRow())+" ");
			System.out.print(new String(kv.getFamily())+":");
			System.out.print(new String(kv.getQualifier())+" ");
			System.out.print(kv.getTimestamp()+" ");
			System.out.println(new String(kv.getValue()));
		}
		table.close();
	}
	/**
	 * 获取表所有记录
	 * @date 2015-07-20 20:13:14
	 * @author sgl
	 * @param tableName 表名
	 * @throws IOException
	 */
	public static void getAllRecord(String tableName) throws IOException{
		HTable table=new HTable(conf,tableName);
		Scan scan=new Scan();
		ResultScanner ss=table.getScanner(scan);
		for(Result r:ss){
			for(KeyValue kv:r.raw()){
				System.out.print(new String(kv.getRow())+" ");
				System.out.print(new String(kv.getFamily())+":");
				System.out.print(new String(kv.getQualifier())+" ");
				System.out.print(kv.getTimestamp()+" ");
				System.out.println(new String(kv.getValue()));
			}
		}
		table.close();
	}
	/**
	 * 删除数据
	 * @date 2015-07-20 20:19:03
	 * @author sgl
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	public static void delRecord(String tableName,String rowKey) throws IOException{
		HTable table=new HTable(conf, tableName);
		List<Delete>list=new ArrayList<Delete>();
		Delete del=new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("删除记录 "+rowKey+" 成功！");
		table.close();
	}
	
	
	public static void main(String[] args) {
		try {
			//创建表
			String tableName="scores";
			String[] familys={"grade","course"};
			HBaseDemo.createTable(tableName, familys);
			
			//添加数据
			HBaseDemo.addRecourd(tableName, "sgl", "grade", "", "5");
			HBaseDemo.addRecourd(tableName, "sgl", "course", "", "90");
			HBaseDemo.addRecourd(tableName, "sgl", "course", "math", "97");
			HBaseDemo.addRecourd(tableName, "sgl", "course", "art", "87");
			HBaseDemo.addRecourd(tableName, "guoguo", "grade", "", "4");
			HBaseDemo.addRecourd(tableName, "guoguo", "course", "math", "89");
			
			System.out.println("**********获取一行数据**********");
			HBaseDemo.getOneRecord(tableName, "sgl");
			
			System.out.println("**********获取所有数据**********");
			HBaseDemo.getAllRecord(tableName);
			
			System.out.println("**********删除一条数据**********");
			HBaseDemo.delRecord(tableName, "guoguo");
			
			System.out.println("**********查看所有数据**********");
			HBaseDemo.getAllRecord(tableName);
			
			//删除表
			HBaseDemo.deleteTable(tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

