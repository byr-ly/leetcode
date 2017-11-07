package com.eb.bi.rs.mras2.correlation.datapro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.eb.bi.rs.frame2.idoxframe.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.idoxframe.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.idoxframe.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.idoxframe.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.correlation.datapro.BookFilter;

public class BookFilter {
	
	private String hkBooks;
	private static Set<String> bookSet = null;
	private static List<String> bookList = null;
	
	@SuppressWarnings("unchecked")
	public BookFilter(String hkBooks) {
		this.hkBooks = hkBooks;
		bookSet = hkBookSets(this.hkBooks);
		bookList = hkBookList();
	}
	
	public List hkBookList() {
		List<String> bookList = new ArrayList<String>();
		Iterator iter = bookSet.iterator();
		while(iter.hasNext()) {
			bookList.add((String) iter.next());
		}
		return bookList;
	}
	
	//将单个文件下的图书放入HashSet中
	public void hkBookSet(File file, Set<String> bookSet) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String temp;
			while((temp = reader.readLine()) != null) {
				bookSet.add(temp);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	//循环调用，将文件夹下的所有文件放入HashSet中
	public Set hkBookSets(String input) {
		File inputPath = new File(input);
		Set<String> bookSet = new HashSet<String>();
		List<File> myFile = new ArrayList<File>();
		listDirectory(inputPath, myFile);
		for(int i = 0; i < myFile.size(); i++) {
			hkBookSet(myFile.get(i), bookSet);
		}
		return bookSet;
	}
	
	//递归获取某个目录下的所有文件，并将结果放入List中
	public void listDirectory(File path, List<File> myFile) {
		if(path.exists()) {
			if(path.isFile()) {
				myFile.add(path);
			} else {
				File[] files = path.listFiles();
				for(int i = 0; i < files.length; i++) {
					listDirectory(files[i], myFile);
				}
			}
		}
	}
	
	
	public static String getRandomBook() {
		Random random = new Random();
		int length = bookList.size();
		int index = Math.abs(random.nextInt()) % length;
		return bookList.get(index);
	}
	
	public static void bookFilters(String inputFile, String outputFile) {
		File input = new File(inputFile);
		File output = new File(outputFile);
		BufferedReader reader = null;
		FileWriter fw = null;
		if(!output.exists()) {
			try {
				output.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			reader = new BufferedReader(new FileReader(input));
			fw = new FileWriter(output);
			String line;
			Boolean isWrite;
			while((line = reader.readLine()) != null) {
				String[] items = line.split("\\|", -1);
				isWrite = true;
				StringBuffer sb = new StringBuffer();
				int count = 0;
				if(items.length > 0) {
					if(!bookSet.contains(items[0]))
						isWrite = false;
					else {
						//拼接过滤后图书的结果，生成图书的格式为：图书A；图书B，相似度|图书C，相似度|...|图书X，相似度
						sb.append(items[0]).append(";");
						for(int i = 1; i < items.length - 1; i+= 2) {
							if(bookSet.contains(items[i])) {
								sb.append(items[i] + "," + items[i+1] + "|");
								count++;
							}
						}
						//过滤后结果如果不足50本的话，就需要从香港书单中随机选取图书补足50本
						if(count < 50) {
							for(int j = count; j < 50; j++) {
								String book = getRandomBook();
								sb.append(book + "," + "10.0%" + "|");
							}
						}
						
						if(sb.length() > 1)
							sb.deleteCharAt(sb.length() - 1);
						sb.append("\n");
					}
					if(isWrite) {
						fw.write(sb.toString());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public void copyAndProcess(String input, String output) {
		File src = new File(input);
		File dest = new File(output);
		if(src.isDirectory()) {
			if(!dest.exists()) {
				dest.mkdir();
			}
			File[] lists = src.listFiles();
			for(File list : lists) {
				String newPath = input + File.separator + list.getName();
				String newFilterPath = output + File.separator + list.getName();
				copyAndProcess(newPath, newFilterPath);
			}
		} else if(src.isFile()) {
			bookFilters(input, output);
		}
	}
	
	public static void main(String[] args) {
		
		PluginUtil plugin = PluginUtil.getInstance();
		plugin.init(args);
		PluginConfig config = plugin.getConfig();

		String hkBook = config.getParam("hk_book_list", null);
		String inputPath = config.getParam("input", null);
		String outputPath = config.getParam("output", null);
		
		BookFilter filter = new BookFilter(hkBook);
		filter.copyAndProcess(inputPath, outputPath);
	}
}
