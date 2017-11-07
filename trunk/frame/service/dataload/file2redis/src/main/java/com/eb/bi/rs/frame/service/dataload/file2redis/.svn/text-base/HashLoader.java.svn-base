package com.eb.bi.rs.frame.service.dataload.file2redis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;



public class HashLoader extends LoaderBase {

	@Override
	public boolean prepare() throws IOException {
		
		
		BufferedReader br = null;
		BufferedWriter bw = null;
		
		int idx = 0;
		cnt.setRecordcnt(0);
		try {
			for (File file : files) {
				br = new BufferedReader(new FileReader(file));					
				File tmp = new File(file.getParent() + "/_" + file.getName());								
				bw = new BufferedWriter(new FileWriter(tmp));	
				tmpFiles[idx++] = tmp;		
				String line = null;					
				long records = 0;
				
				String field = config.getParam("field", null);
				
				if (!multiGrp) {
					while ((line = br.readLine()) != null) {						
						String hKey = LineParser.getKey(line, key);
						String hField = LineParser.getField(line, field);  //如果是定值。
						String hValue = LineParser.getValue(line, value);
						bw.append(hKey + "|" + hField + "|" +hValue + "\n");
						records++;
					}					
				} else {
					boolean fieldIsConst = config.getParam("filedIsConst", false);				
					if (fieldIsConst) {
						
						while ((line = br.readLine()) != null) {						
							String hKey = LineParser.getKey(line, key);
							List<String> fields = LineParser.getMField(field);  //如果是定值。
							List<String> values = LineParser.getMValue(line, value);
							for (int i = 0; i < fields.size(); i++) {							
								bw.append(hKey + "|" + fields.get(i) + "|" + values.get(i) + "\n");
							}
							records++;
						}						
					}
					
					else {						
						int groupBegIdx = Integer.parseInt(value);
						while ((line = br.readLine()) != null) {
							String hKey = LineParser.getKey(line, key);		
							
							int grpLen = config.getParam("groupLength", 1);
							
							ArrayList<Integer> valLocInGrp = new ArrayList<Integer>();			
			
							for (String index : config.getParam("valueLocInGroup", "0").split(",")) {
								valLocInGrp.add(Integer.parseInt(index));
							}
							ArrayList<Integer> fieldLocInGrp = new ArrayList<Integer>();		
							for (String index : config.getParam("fieldLocInGroup", "0").split(",")) {
								fieldLocInGrp.add(Integer.parseInt(index));
							}
							String delimiter = config.getParam("delimiterInValue", ":");
							
							List<String> fields = LineParser.getFields(line, groupBegIdx, grpLen, fieldLocInGrp, delimiter);
							List<String> values = LineParser.getValues(line, groupBegIdx, grpLen, valLocInGrp, delimiter);
							for (int i = 0; i < fields.size(); i++) {							
								bw.append(hKey + "|" + fields.get(i) + "|" + values.get(i) + "\n");
							}
							records++;
						}					
						
					}
						
				}				
				log.info("file " + file + " contains " + records + " records");
				log.info("record count before:" + cnt.getRecordcnt());
				cnt.setRecordcnt(cnt.getRecordcnt()+records);
				log.info("record count now:" + cnt.getRecordcnt());
				bw.flush();
				bw.close();
			}		
		}finally {					
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}					
			if (bw != null) {	
				try {
					bw.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		}
		return true;	

	}

	@Override
	public boolean loadCore() throws IOException {
		Jedis jedis = null;	
		BufferedReader br = null;
		cnt.setRecordcnt(0);
		try {
			jedis = new Jedis(redisIp, redisPort);
			for (File file : tmpFiles) {				
				br = new BufferedReader( new FileReader(file));					
				String line = null;
				long records = 0;					
				Pipeline pipeline = jedis.pipelined();				
				while ((line = br.readLine()) != null) {					
					String[] pairs = line.split("\\|", 3);	/*这有一个问题，就是如果field是由多个字段组成的，且分隔符是|，就会有问题*/
					pipeline.hset(pairs[0], pairs[1], pairs[2]);
					records++;
				}
				
				
				log.info("tmp file " + file + " contains " + records + " key value pair");						
				if (records > 0) {					
					pipeline.sync();
				}	
				log.info("key value pair count before:" + cnt.getRecordcnt());
				cnt.setRecordcnt(cnt.getRecordcnt()+records);
				log.info("key value pair count now:" + cnt.getRecordcnt());						
				file.delete();
			}			
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (Exception e) {
					e.printStackTrace();
				}				
			}
			if (jedis != null) {
				jedis.close();				
			}
		}
		return true;		

	}	
}




