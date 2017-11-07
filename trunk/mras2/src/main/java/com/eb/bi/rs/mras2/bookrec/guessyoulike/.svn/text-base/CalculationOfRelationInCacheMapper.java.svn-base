package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.Similar;
import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.Similars;

public class CalculationOfRelationInCacheMapper extends Mapper<NullWritable, Text, Text, Text>{//LongWritable
	//图书相似度数据
	private Map<String,Similars> book_Similars = new HashMap<String,Similars>();

	private String field_separator; 
	private String inner_separator;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
			
		//分隔符加载
		field_separator =context.getConfiguration().get("Appconf.data.field.separator");
		inner_separator =context.getConfiguration().get("Appconf.data.inner.separator");
			
		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());		
		//URI[] localFiles = context.getCacheFiles();
		
		LocalFileSystem fs = FileSystem.getLocal(context.getConfiguration());
		
		System.out.println(localFiles.length);//test
		
		for(int i = 0; i < localFiles.length; i++){
			//String line;
			//BufferedReader in = null;
			//try {
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, localFiles[i], context.getConfiguration());;
				
				//NullWritable key=null;
				Text val=new Text();
				
				int numtest = 0;
				
				while(reader.next(NullWritable.get(), val)){
					//图书A|图书B|相似度向量
					
					System.out.println(numtest++);//test
					
					String[] fields;
					if(field_separator.equals("|")){
						fields = val.toString().split("\\|");
					} else{
						fields = val.toString().split(field_separator);
					}
					
					Similars similars = new Similars();
					
					if(book_Similars.containsKey(fields[0])){
						similars = book_Similars.get(fields[0]);
						similars.add(fields[1], fields[2], inner_separator);
						book_Similars.put(fields[0], similars);
					} else{
						similars.add(fields[1], fields[2], inner_separator);
						book_Similars.put(fields[0], similars);
					}
				}
				/*
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				//if(localFiles[i].toString().contains("booksimilar")) {
				while ((line = in.readLine()) != null) {
					//图书A|图书B|相似度向量
					String fields[] = line.split("\\|");
					
					Similars similars = new Similars();
					
					if(book_Similars.containsKey(fields[0])){
						similars = book_Similars.get(fields[0]);
						similars.add(fields[1], fields[2], inner_separator);
						book_Similars.put(fields[0], similars);
					} else{
						similars.add(fields[1], fields[2], inner_separator);
						book_Similars.put(fields[0], similars);
					}
				}
				*/
			//}
			/*
			} finally {
				if (in != null) {
					in.close();
				}			
			}
			*/			
		}
	}
		
	/**
	 * 
	 * */
	@Override
	protected void map(NullWritable key, Text value, Context context) 
			throws IOException, InterruptedException{//LongWritable
		//用户|源图书|图书打分|来源集
		String[] fields;
		if(field_separator.equals("|")){
			fields = value.toString().split("\\|");
		} else{
			fields = value.toString().split(field_separator);
		}

		//用户
		String userId = fields[0];
		//源图书
		String sBook = fields[1];
		//图书打分
		float sbookScore = Float.valueOf(fields[2]);
		//来源集
		String[] sources = fields[3].toString().split(inner_separator);
		
		//关联：源图书|目标图书|相似度向量
		if(!book_Similars.containsKey(sBook)){
			return;
		}

		Map<String,Similar> sbookSimilars = book_Similars.get(sBook).getSimilars();

		Iterator<Entry<String, Similar>> it = sbookSimilars.entrySet().iterator();
		while (it.hasNext()){
			Map.Entry<String, Similar> entry = (Map.Entry<String, Similar>) it.next();
			String pbook;
			pbook = entry.getKey();//待推荐图书
			Similar pSimilar = new Similar();
			pSimilar = entry.getValue();//相似度向量
			
			pSimilar.similarMult(sbookScore);
			
			String similarString = "";
			similarString = pSimilar.tosimilarString(inner_separator);
			for(int j =0; j != sources.length; j++){
				//key:用户|待推荐图书|源图书来源;val:源图书|计算后相似度向量
				context.write(new Text(userId+field_separator+pbook+field_separator+sources[j]),new Text(sBook+field_separator+similarString));
			}
		}
	}
}
