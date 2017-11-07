package com.eb.bi.rs.frame.service.dataload.file2redis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class CopyOfListLoader
{
	private static File[] tmpFiles;
	public static boolean prepareData(String fileName, String key, String value, LongBean cnt,boolean multiVal, int stepLen)
	{
		
		File file = new File(fileName);
		
		File[] files = null;
		
		if (!file.exists())
		{
			return false;			
		}
		
		if (file.isDirectory()) {
			
			FileFilter filter = new FileFilter() {				
				public boolean accept(File pathname) {
					return !pathname.getName().endsWith(".crc");
				}
			};
			
			files = file.listFiles(filter);
		}
		else 
		{
			files = new File[]{file};
		}
		tmpFiles = new File[files.length];
		
		FileReader fr = null;
		BufferedReader br = null;
		
		FileWriter fw = null;
		BufferedWriter bw = null;
		
		int i = 0;
		cnt.setRecordcnt(0);
		if (!multiVal) {
			for (File oneFile : files)
			{
				try
				{
					
					fr = new FileReader(oneFile);
					br = new BufferedReader(fr);
					
					File f = new File(oneFile.getParent() + "/_" + oneFile.getName());
					tmpFiles[i++] = f;
					if (f.exists())
					{
						f.delete();
						f.createNewFile();
					}
					
					fw = new FileWriter(f, true);
					bw = new BufferedWriter(fw);
					
					String line = null;
					
					long records = 0;
					
					while ((line = br.readLine()) != null) {					
						String lKey = getKey(line, key);
						String lValue = getValue(line, value);
						bw.append(lKey + "|" + lValue + "\n");	
						records++;
					}					
					bw.flush();
					cnt.setRecordcnt(cnt.getRecordcnt()+records);
					fr.close();
					br.close();
					fw.close();
					bw.close();
				}
				catch (Exception e)
				{
					try
					{
						e.printStackTrace();
						
						if (fr != null)
						{
							fr.close();
							fr = null;
						}
						
						if (br != null)
						{
							br.close();
							br = null;
						}
						
						if (fw != null)
						{
							fw.close();
						}
						
						if (bw != null)
						{
							bw.close();
						}
					}
					catch (Exception ee)
					{
						ee.printStackTrace();
						return false;
					}
					
					return false;
				}
				
			}
			
		}else {
			int valBegIdx = Integer.parseInt(value);
			for (File oneFile : files)
			{
				try
				{
					
					fr = new FileReader(oneFile);
					br = new BufferedReader(fr);
					
					File f = new File(oneFile.getParent() + "/_" + oneFile.getName());
					tmpFiles[i++] = f;
					if (f.exists())
					{
						f.delete();
						f.createNewFile();
					}
					
					fw = new FileWriter(f, true);
					bw = new BufferedWriter(fw);
					
					String line = null;
					
					long records = 0;					
					while ((line = br.readLine()) != null) {
						String lKey = getKey(line, key);
						List<String> values = getValues(line, valBegIdx, stepLen);
						for (String lValue : values) {
							bw.append(lKey + "|" + lValue + "\n");						
						}
						records++;
					}	
					
					System.out.println("file " + oneFile + " contains " + records + " records");
					bw.flush();
					System.out.println("record count before:" + cnt.getRecordcnt());
					cnt.setRecordcnt(cnt.getRecordcnt()+records);
					System.out.println("record count now:" + cnt.getRecordcnt());
					fr.close();
					br.close();
					fw.close();
					bw.close();
				}
				catch (Exception e)
				{
					try
					{
						e.printStackTrace();
						
						if (fr != null)
						{
							fr.close();
							fr = null;
						}
						
						if (br != null)
						{
							br.close();
							br = null;
						}
						
						if (fw != null)
						{
							fw.close();
						}
						
						if (bw != null)
						{
							bw.close();
						}
					}
					catch (Exception ee)
					{
						ee.printStackTrace();
						return false;
					}
					
					return false;
				}
				
			}
			
		}
		
		
		return true;
	}
	
	
	
	
	
	
	public static boolean load(String fileName, String redisIp, Integer redisPort, LongBean cnt, String command)
	{
		Jedis redis = null;
		redis = new Jedis(redisIp, redisPort);
		
		cnt.setRecordcnt(0);
		if ("lpush".equalsIgnoreCase(command)) {
			for (File oneFile : tmpFiles)
			{
				if (!oneFile.exists())
				{
					continue;			
				}
				try 
				{
					FileReader fr = null;
					BufferedReader br = null;
					
					try 
					{
						fr = new FileReader(oneFile);
						br = new BufferedReader(fr);
						
						String line = null;
						long records = 0;
						
						Pipeline pipeline = redis.pipelined();
						while ((line = br.readLine()) != null) 
						{
							String[] pairs = line.split("\\|");	
							pipeline.lpush(pairs[0], pairs[1]);
							records++;
						}
						if (records > 0)
						{
							pipeline.sync();
						}
						
						cnt.setRecordcnt(cnt.getRecordcnt()+records);
						fr.close();
						br.close();
						oneFile.delete();
					}
					catch (Exception e)
					{
						try
						{
							e.printStackTrace();
							
							if (fr != null)
							{
								fr.close();
								fr = null;
							}
							
							if (br != null)
							{
								br.close();
								br = null;
							}	
							
							if (redis != null)
							{
								redis.close();
								redis = null;
							}
						}
						catch (Exception anotherException)
						{
							anotherException.printStackTrace();
						}
					}
					
				} 
				catch (Exception e) 
				{
					e.printStackTrace();
				}
			}
		}else if ("rpush".equalsIgnoreCase(command)){
			for (File oneFile : tmpFiles)
			{
				
				if (!oneFile.exists())
				{
					continue;			
				}
				try 
				{
					FileReader fr = null;
					BufferedReader br = null;
					
					try 
					{
						fr = new FileReader(oneFile);
						br = new BufferedReader(fr);
						
						String line = null;
						long records = 0;
						
						Pipeline pipeline = redis.pipelined();
						while ((line = br.readLine()) != null) 
						{
							String[] pairs = line.split("\\|",-1);/*此处可能存在空的情况*/											
							pipeline.rpush(pairs[0], pairs[1]);
							records++;
						}
						System.out.println("tmp file " + oneFile + " contains " + records + " row");
						if (records > 0)
						{
							pipeline.sync();
						}						
						
						System.out.println("row count before:" + cnt.getRecordcnt());
						cnt.setRecordcnt(cnt.getRecordcnt()+records);
						System.out.println("row count now:" + cnt.getRecordcnt());						
						fr.close();
						br.close();
						oneFile.delete();
					}
					catch (Exception e)
					{
						try
						{
							e.printStackTrace();
							
							if (fr != null)
							{
								fr.close();
								fr = null;
							}
							
							if (br != null)
							{
								br.close();
								br = null;
							}	
							
							if (redis != null)
							{
								redis.close();
								redis = null;
							}
						}
						catch (Exception anotherException)
						{
							anotherException.printStackTrace();
						}
					}
					
				} 
				catch (Exception e) 
				{
					e.printStackTrace();
				}
			}
			
		}
		
		redis.close();
		return true;
	}
	
	
	
	/**
	 * get key using key pattern from input line.
	 * @param line  delimited by | 
	 * @param key   delimited by $
	 * @return
	 */
	private static String getKey(String line, String key)
	{
		String[] fields = line.split("\\|");
		String resultKey = key;
		
		for (int i = 0; i < fields.length; i++)
		{
			String replacer = "$(" + i + ")";
			resultKey = resultKey.replace(replacer, fields[i]);
		}
		
		return resultKey;
	}
	
	
	private static String getValue(String line, String value)
	{
		String[] fields = line.split("\\|",-1);
		String resultValue = value;
		
		for (int i = 0; i < fields.length; i++)
		{
			String replacer = "$(" + i + ")";
			resultValue = resultValue.replace(replacer, fields[i]);
		}
		
		return resultValue;
	}
	
	
	private static List<String> getValues(String line, int value, int stepLen)
	{
		String[] fields = line.split("\\|",-1);
		List<String> values = new ArrayList<String>();
		
		for (int i = value; i < fields.length; i += stepLen) {
	
			if (stepLen == 1) {
				values.add(fields[i]);
			}else {
				StringBuffer sb = new StringBuffer(fields[i]);
				for (int j = i + 1; j < i + stepLen; j++) {
					sb.append(":" + fields[j]);//分隔符可配置
				}
				values.add(sb.toString());
			}			
		}
		
		return values;
	}
}

//public class ListLoader
//{
//	private static File[] tmpFiles;
//	public static boolean prepareData(String fileName, String key, String value, LongBean cnt)
//	{
//		
//		File file = new File(fileName);
//		
//		File[] files = null;
//		
//		if (!file.exists())
//		{
//			return false;			
//		}
//		
//		if (file.isDirectory())
//		{
//			files = file.listFiles();
//		}
//		else 
//		{
//			files = new File[]{file};
//		}
//		tmpFiles = new File[files.length];
//		
//		FileReader fr = null;
//		BufferedReader br = null;
//		
//		FileWriter fw = null;
//		BufferedWriter bw = null;
//		
//		int i = 0;
//		cnt.setRecordcnt(0);
//		for (File oneFile : files)
//		{
//			try
//			{
//				
//				fr = new FileReader(oneFile);
//				br = new BufferedReader(fr);
//				
//				File f = new File(oneFile.getParent() + "/_" + oneFile.getName());
//				tmpFiles[i++] = f;
//				if (f.exists())
//				{
//					f.delete();
//					f.createNewFile();
//				}
//				
//				fw = new FileWriter(f, true);
//				bw = new BufferedWriter(fw);
//				
//				String line = null;
//				
//				long records = 0;
//				
//				
//				while ((line = br.readLine()) != null)
//				{
//					String lKey = getKey(line, key);
//					String lValue = getValue(line, value);
//					bw.append(lKey + "|" + lValue + "\n");	
//					records++;
//				}
//				bw.flush();
//				cnt.setRecordcnt(cnt.getRecordcnt()+records);
//				fr.close();
//				br.close();
//				fw.close();
//				bw.close();
//			}
//			catch (Exception e)
//			{
//				try
//				{
//					e.printStackTrace();
//					
//					if (fr != null)
//					{
//						fr.close();
//						fr = null;
//					}
//					
//					if (br != null)
//					{
//						br.close();
//						br = null;
//					}
//					
//					if (fw != null)
//					{
//						fw.close();
//					}
//					
//					if (bw != null)
//					{
//						bw.close();
//					}
//				}
//				catch (Exception ee)
//				{
//					ee.printStackTrace();
//					return false;
//				}
//				
//				return false;
//			}
//			
//		}
//		
//		return true;
//	}
//	
//	
//	public static boolean load(String fileName, String redisIp, Integer redisPort, LongBean cnt)
//	{
//		Jedis redis = null;
//		redis = new Jedis(redisIp, redisPort);
//		
//		cnt.setRecordcnt(0);
//		for (File oneFile : tmpFiles)
//		{
//			if (!oneFile.exists())
//			{
//				continue;			
//			}
//			try 
//			{
//				FileReader fr = null;
//				BufferedReader br = null;
//				
//				try 
//				{
//					fr = new FileReader(oneFile);
//					br = new BufferedReader(fr);
//					
//					String line = null;
//					long records = 0;
//					
//					Pipeline pipeline = redis.pipelined();
//					while ((line = br.readLine()) != null) 
//					{
//						String[] pairs = line.split("\\|");												
//						pipeline.lpush(pairs[0], pairs[1]);
//						records++;
//					}
//					if (records > 0)
//					{
//						pipeline.sync();
//					}
//					
//					cnt.setRecordcnt(cnt.getRecordcnt()+records);
//					fr.close();
//					br.close();
//					oneFile.delete();
//				}
//				catch (Exception e)
//				{
//					try
//					{
//						e.printStackTrace();
//						
//						if (fr != null)
//						{
//							fr.close();
//							fr = null;
//						}
//						
//						if (br != null)
//						{
//							br.close();
//							br = null;
//						}	
//						
//						if (redis != null)
//						{
//							redis.close();
//							redis = null;
//						}
//					}
//					catch (Exception anotherException)
//					{
//						anotherException.printStackTrace();
//					}
//				}
//				
//			} 
//			catch (Exception e) 
//			{
//				e.printStackTrace();
//			}
//		}
//		redis.close();
//		return true;
//	}
//	
//	
//	
//	/**
//	 * get key using key pattern from input line.
//	 * @param line  delimited by | 
//	 * @param key   delimited by $
//	 * @return
//	 */
//	private static String getKey(String line, String key)
//	{
//		String[] fields = line.split("\\|");
//		String resultKey = key;
//		
//		for (int i = 0; i < fields.length; i++)
//		{
//			String replacer = "$(" + (i+1) + ")";
//			resultKey = resultKey.replace(replacer, fields[i]);
//		}
//		
//		return resultKey;
//	}
//	
//	
//	private static String getValue(String line, String value)
//	{
//		String[] fields = line.split("\\|");
//		String resultValue = value;
//		
//		for (int i = 0; i < fields.length; i++)
//		{
//			String replacer = "$(" + (i+1) + ")";
//			resultValue = resultValue.replace(replacer, fields[i]);
//		}
//		
//		return resultValue;
//	}
//}




