package com.eb.bi.rs.mras.frame.service.dataload.hdfs2redis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;



/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
    	
    	long start = System.currentTimeMillis();
    	Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FileSystem local = FileSystem.getLocal(conf);
       

        Path localFile = new Path("d:/test");
       FSDataOutputStream out = local.create(localFile);
        FileStatus[] inputFiles = hdfs.globStatus(new Path("hdfs://10.1.1.240:9000/user/hadoop/searchkeyoff/normalizedtopktagweight/part-*"));
       
       System.out.println(inputFiles.length);
        for(int i = 0; i < inputFiles.length; i++){
    	   FSDataInputStream in = hdfs.open(inputFiles[i].getPath());
    	   byte[] buffer = new byte[256];
    	   int bytesRead = 0;
    	   while((bytesRead = in.read(buffer)) > 0){
    		   out.write(buffer);
    	   }
    	   in.close();    	   
        }
        out.close();
        System.out.println(System.currentTimeMillis()-start);
        
        start = System.currentTimeMillis();
        
  
        BufferedReader in = new BufferedReader(new FileReader(localFile.toString()));
        String lineString;
    	 JedisPool pool = null;
         Jedis jedis = null;
         
         boolean borrowOrOprSuccess = true;
         try {
             pool = RedisPoolAPI.getPool();
             jedis = pool.getResource();
             while((lineString = in.readLine()) != null){            	
             	String[] fields	= lineString.split("\\|"); 
             	String key = "dm_searchword_tag_weight_hash:" + fields[0];            	  

             	Transaction tranc = jedis.multi();
             	tranc.del(key);
             	tranc.hset(key, fields[1], fields[2]); 
             	tranc.exec();           	
             }

         } catch (JedisConnectionException e) {
             borrowOrOprSuccess = false;
             if (jedis != null)
                 pool.returnBrokenResource(jedis);

         } finally {
             if (borrowOrOprSuccess)
                RedisPoolAPI.returnResource(pool,jedis);
         }       
         System.out.println(System.currentTimeMillis()-start);  
    }
}
