package com.eb.bi.rs.mras2.hkmras.hkguessyoulike.filler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.eb.bi.rs.frame2.idoxframe.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.idoxframe.pluginutil.PluginUtil;

import org.apache.log4j.Logger;


public class GuessYouLikeFillerforHongKong {
	
	
	public static void main(String[] args) {
		
		
		PluginUtil plugin = PluginUtil.getInstance();
		plugin.init(args);
		PluginConfig config = plugin.getConfig();
		Logger log = plugin.getLogger();
		
		String SourceFilePath = config.getParam("input_path", null);
		String TargetFilePath = config.getParam("output_path", null);
		int numberOfFiller = Integer.parseInt(config.getParam("numberOfFiller", null));
		int numberOfBooks = Integer.parseInt(config.getParam("numberOfBooks", null));

		if (SourceFilePath == null || TargetFilePath == null) {
			log.error("Config error, local_path or hdfs_path is null, please check it");
			System.exit(-1);
		}
		
		try {
			
			List<String> bookList = getBooks(SourceFilePath);
			
			File file = new File(TargetFilePath);
			if(file.exists()){
				file.delete();
			}
			file.createNewFile();
			FileOutputStream fout = new FileOutputStream(file);
			Writer w = new OutputStreamWriter(fout, "utf-8");
			PrintWriter pw = new PrintWriter(w);
			
			int num = 1;
			while (num <= numberOfFiller) {
				pw.println( generateFiller(bookList,num,numberOfBooks ) );
				num++;
				Thread.sleep(1l);
			}
			pw.flush();
			pw.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			log.error(e);
			e.printStackTrace();
		}
	
	}
	
	public static List<String> getBooks(String SourceFilePath) throws Exception{
				
			List<String> bookList = new ArrayList<String>();
			File file = new File(SourceFilePath);
			File[] files = file.listFiles();
			
			for (File f : files) {
				if (f.getName().startsWith(".")) {
					continue ;
				}
				FileInputStream fin = new FileInputStream(f);
				Reader r = new InputStreamReader(fin, "utf-8");
				BufferedReader br = new BufferedReader(r);
				
				String line = null;
				
				while( (line=br.readLine()) !=null){
					bookList.add(line);
				}
				
				br.close();
			}
			return bookList;
	}
	
	public static String generateFiller(List<String> bookList , int num , int numberOfBooks) throws Exception{
				
		long seed = new Date().getTime();
		Random random = new Random(seed);
		 Set<String> bookSet = new HashSet<String>();
		while (bookSet.size() < numberOfBooks) {
			
			int index = random.nextInt(bookList.size());
			String bookId = (String) bookList.get(index);
			bookSet.add(bookId);
			
		}
		
		StringBuilder books = new StringBuilder();
		books.append(num + ";");
		
		for (String bookId : bookSet) {
			books.append(bookId + ",|");
		}
		return books.toString();
		
	}
	
}
