package com.eb.bi.rs.mras2.cartoonrec.corelationrec.RealTimeFillerDataPrepare;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.StringIntPair;
 
public class RealTimeFillerDataPrepareReducer extends Reducer<Text, Text, Text, NullWritable>{
	
  private int hotBookCount;
  private HashMap<String, TreeSet<StringIntPair>> classHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
  
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
  
	    for(Text value : values){
	    	String[] fields = value.toString().split("\\|", -1);
			if(fields.length == 6) {
				String bookId = fields[0];
				String classType = fields[2];
				StringIntPair pair = new StringIntPair(bookId,Integer.parseInt(fields[5]));	
				if(!classType.equals("")){
					if (classHotBookMap.containsKey(classType)) {
						TreeSet<StringIntPair> hotBookset = classHotBookMap.get(classType);	
						if (hotBookset.size() < hotBookCount) {
							hotBookset.add(pair);
						} else if (pair.compareTo(hotBookset.last()) < 0) {									
							hotBookset.remove(hotBookset.last());
							hotBookset.add(pair);
						}
					}else {
						TreeSet<StringIntPair> hotBookset = new TreeSet<StringIntPair>();
						hotBookset.add(pair);
						classHotBookMap.put(classType, hotBookset);							
					}
				}
			}
	    }	
	    for (Entry<String, TreeSet<StringIntPair>> entry : classHotBookMap.entrySet()) {
			StringBuffer bookset = new StringBuffer();
			Iterator<StringIntPair> it= entry.getValue().iterator();
			while(it.hasNext()){
				bookset.append(it.next().getFirst()+"|");
			}
			context.write(new Text(entry.getKey().toString()+";"+bookset),NullWritable.get()); 
		} 
	    
  }   
   @Override
   protected void setup(Context context) throws IOException, InterruptedException { 
	 Configuration conf = context.getConfiguration();
	 this.hotBookCount = conf.getInt("hot.book.count", 200);
   }
}


