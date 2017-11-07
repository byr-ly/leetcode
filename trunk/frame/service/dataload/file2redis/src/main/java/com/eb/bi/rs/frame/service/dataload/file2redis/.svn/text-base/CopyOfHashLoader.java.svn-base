package com.eb.bi.rs.frame.service.dataload.file2redis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * @author zzl
 */

public class CopyOfHashLoader 
{
	private static File[] tmpFiles;

	public static boolean prepareData(String fileName, String key, String field, String value, LongBean cnt)
	{
		File file = new File(fileName);
		
		File[] files = null;
		
		if (!file.exists())
		{
			return false;			
		}
		
		if (file.isDirectory())
		{
			files = file.listFiles();
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
				
				while ((line = br.readLine()) != null)
				{
					String hKey = getKey(line, key);
					String hField = getField(line, field);
					String hValue = getValue(line, value);
					bw.append(hKey + "|" + hField + "|" +hValue + "\n");
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
		
		return true;
	}
	public static boolean load(String fileName, String redisIp, Integer redisPort, LongBean cnt)
	{
		Jedis redis = null;
		redis = new Jedis(redisIp, redisPort);
		
		cnt.setRecordcnt(0);
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
						pipeline.hset(pairs[0], pairs[1], pairs[2]);
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
			String replacer = "\\$\\(" + (i+1) + "\\)"; //替换$i为特定字段的值
			resultKey = resultKey.replaceAll(replacer, fields[i]);
		}
		
		return resultKey;
	}
	
	
	private static String getValue(String line, String value)
	{
		String[] fields = line.split("\\|");
		String resultValue = value;
		
		for (int i = 0; i < fields.length; i++)
		{
			String replacer = "$(" + (i+1) + ")";
			resultValue = resultValue.replace(replacer, fields[i]);
		}
		
		return resultValue;
	}

	private static String getField(String line, String field)
	{
		String[] fields = line.split("\\|");
		String resultField = field;
		
		for (int i = 0; i < fields.length; i++)
		{
			String replacer = "$(" + (i+1) + ")";
			resultField = resultField.replace(replacer, fields[i]);
		}
		
		return resultField;
	}
}

