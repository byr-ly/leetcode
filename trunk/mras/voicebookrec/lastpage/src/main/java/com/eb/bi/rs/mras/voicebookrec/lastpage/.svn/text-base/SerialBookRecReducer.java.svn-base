package com.eb.bi.rs.mras.voicebookrec.lastpage;


import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SerialBookRecReducer extends Reducer<Text, Text, Text, Text> {
	
	
	private int n = 2;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
		/*图书ID|系列ID|系列顺序|栏目ID*/			
		List<String> valueList = new ArrayList<String>();
		for(Text value : values) {
			valueList.add(value.toString());
		}
		int listSize = valueList.size();
		String sequence = valueList.get(0).split("\\|", -1)[2];//根据第一个元素判断是该系列是否有顺序
		if("0".equals(sequence)){//系列无顺序
			Random random = new Random(new Date().getTime());
			if(listSize > n + 1){//可以随机选择出n本书				
				for(int i = 0; i < listSize; ++i){
					String bookId1 = valueList.get(i).split("\\|", -1)[0];
					Set<Integer> picked = new HashSet<Integer>();
					int count = 0;
					while(count < n) {	
						int randIdx = random.nextInt(listSize);
						while(randIdx == i || picked.contains(randIdx)) {
							randIdx = random.nextInt(listSize);
						}
						picked.add(randIdx);
						String bookId2 = valueList.get(randIdx).split("\\|", -1)[0];
						context.write(new Text(bookId1), new Text(bookId2));						
						++count;
					}
				}				
			}
			else {
				for(int i = 0; i < listSize; ++i){
					String bookId1 = valueList.get(i).split("\\|", -1)[0];
					for(int j = 0; j < listSize; ++j){
						if(i != j){
							String bookId2 = valueList.get(j).split("\\|", -1)[0];
							context.write(new Text(bookId1), new Text(bookId2));		
						}
					}
				}			
			}		
		}
		else {//系列有顺序
			Collections.sort(valueList, new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					int a = Integer.parseInt(o1.split("\\|", -1)[2]) ;
					int b = Integer.parseInt(o2.split("\\|", -1)[2]) ;					
					return (a < b ? -1 : (a == b ? 0 : 1));

				}				
			});
			for(int i = 0; i < listSize; ++i){
				String bookId1 = valueList.get(i).split("\\|", -1)[0];				
				for(int j = i + 1; j < listSize && j < i + n +1 ; ++j){
					String bookId2 = valueList.get(j).split("\\|", -1)[0];				
					context.write(new Text(bookId1), new Text(bookId2));
				}				
			}			
		}
	}
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		super.setup(context);
		n = context.getConfiguration().getInt("read.recommend.serial.book.number", 2);		
	}

}
