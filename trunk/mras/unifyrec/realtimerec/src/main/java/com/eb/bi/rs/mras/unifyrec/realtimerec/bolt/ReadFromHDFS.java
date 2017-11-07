package com.eb.bi.rs.mras.unifyrec.realtimerec.bolt;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author ynn
 * @date 创建时间：2015-10-22 下午5:19:13
 * @version 1.0
 */
public class ReadFromHDFS {
	
	public static Map<String, Float> getHDFSScore(String hdfsPath,
			String hdfsFileNames) {
		Map<String, Float> bookScore = new HashMap<String, Float>();
		try {
			File workaround = new File(".");
			System.getProperties().put("hadoop.home.dir",
					workaround.getAbsolutePath());
			new File("./bin").mkdirs();
			new File("./bin/winutils.exe").createNewFile();
			Configuration conf = new Configuration();
			conf.set("fs.default.name", hdfsPath);
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] inputFiles = fs.globStatus(new Path(hdfsFileNames));
			FSDataInputStream hdfsInStream = null;
			for (FileStatus inputFile : inputFiles) {
				Path filePath = inputFile.getPath();
				hdfsInStream = fs.open(filePath);
				String line = hdfsInStream.readLine();
				while (line != null) {
					String[] strs = line.split("\\|");
					if (strs.length == 2) {
						bookScore.put(strs[0], Float.parseFloat(strs[1]));
					}
					line = hdfsInStream.readLine();
				}
				System.out.println(bookScore);
			}
			if (hdfsInStream != null) {
				hdfsInStream.close();
			}
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bookScore;
	}

	public static List<String> getHDFSFiller(String hdfsPath,
			String hdfsFileNames) {
		List<String> fillerBooks = new ArrayList<String>();
		try {
			File workaround = new File(".");
			System.getProperties().put("hadoop.home.dir",
					workaround.getAbsolutePath());
			new File("./bin").mkdirs();
			new File("./bin/winutils.exe").createNewFile();
			Configuration conf = new Configuration();
			conf.set("fs.default.name", hdfsPath);
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] inputFiles = fs.globStatus(new Path(hdfsFileNames));
			FSDataInputStream hdfsInStream = null;
			for (FileStatus inputFile : inputFiles) {
				Path filePath = inputFile.getPath();
				hdfsInStream = fs.open(filePath);
				String line = hdfsInStream.readLine();
				while (line != null) {
					fillerBooks.add(line);
					line = hdfsInStream.readLine();
				}
				System.out.println(fillerBooks);
			}
			if (hdfsInStream != null) {
				hdfsInStream.close();
			}
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fillerBooks;
	}
}
