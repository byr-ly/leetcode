package com.eb.bi.rs.mras.seachrec.relatedkey.offline;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;

public class KeywordGenerateMapper extends Mapper<Text, Text, Text, Text>{//分词判定并分词
	private Set<String> m_book_author_nameList = new HashSet<String>();//图书名称,作者名称表
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		//载入图书名称列表,作者名称列表
		super.setup(context);

		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());		

		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while ((line = in.readLine()) != null) {					
					String fields[] = line.split("\\|");
					m_book_author_nameList.add(fields[0]);
				}

			} finally {
				if (in != null) {
					in.close();
				}			
			}			
		}
	}
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String sentence = value.toString();	
		
		if(m_book_author_nameList.contains(sentence)){
			context.write(key,value);
		}
		else{
			JiebaSegmenter segmenter = new JiebaSegmenter();
			List<SegToken> tokens = segmenter.process(sentence, SegMode.SEARCH);
			for(SegToken token: tokens){
				String keyword = token.token;
				
				if(keyword.length() > 6 || keyword.length() < 2)
					continue;
				
				context.write(key,new Text(keyword));
			}
		}
	}
}
