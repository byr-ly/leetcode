package com.eb.bi.rs.mras2.bookrec.corelationrec.RealTimeFillerDataPrepare;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.StringIntPair;
 
public class RealTimeFillerDataPrepareReducer extends Reducer<Text, Text, Text, NullWritable>{
	
  private int hotBookCount;
  private String authorBookOutputPath;
  private String classBookOutputPath;
  private String bigclassBookOutputPath;
  private MultipleOutputs<Text, NullWritable> out;
  
  private HashMap<String, TreeSet<StringIntPair>> classHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
  private HashMap<String, TreeSet<StringIntPair>> authorHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
  private HashMap<String, TreeSet<StringIntPair>> bigclassHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
  
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
  
	    for(Text value : values){
	    	String[] fields = value.toString().split("\\|", -1);
			if(fields.length == 5) {
				String bookId = fields[0];
				String author = fields[1];
				String classType = fields[2];
				String bigClass = fields[3];

				StringIntPair pair = new StringIntPair(bookId,Integer.parseInt(fields[4]));	
		        if(!author.equals("")){
		        	if (authorHotBookMap.containsKey(author)) {
						TreeSet<StringIntPair> Bookset = authorHotBookMap.get(author);	
							Bookset.add(pair);
					}else {
						TreeSet<StringIntPair> Bookset = new TreeSet<StringIntPair>();
						Bookset.add(pair);
						authorHotBookMap.put(author, Bookset);							
					}
		        }
				
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
				
				
				if(!bigClass.equals("")){
					if (bigclassHotBookMap.containsKey(bigClass)) {
						TreeSet<StringIntPair> hotbigclassBookset = bigclassHotBookMap.get(bigClass);	
						if (hotbigclassBookset.size() < hotBookCount) {
							hotbigclassBookset.add(pair);
						} else if (pair.compareTo(hotbigclassBookset.last()) < 0) {									
							hotbigclassBookset.remove(hotbigclassBookset.last());
							hotbigclassBookset.add(pair);
						}
					}else {
						TreeSet<StringIntPair> hotbigclassBookset = new TreeSet<StringIntPair>();
						hotbigclassBookset.add(pair);
						bigclassHotBookMap.put(bigClass, hotbigclassBookset);							
					}
				}	
			}	
	    }						
  }   
   @Override
   protected void setup(Context context) throws IOException, InterruptedException { 
	 out = new MultipleOutputs<Text, NullWritable>(context);
	 Configuration conf = context.getConfiguration();
	 this.hotBookCount = conf.getInt("hot.book.count", 200);
	 this.authorBookOutputPath = conf.get("author.book.output.path", "");
	 this.classBookOutputPath = conf.get("class.book.output.path", "");
	 this.bigclassBookOutputPath = conf.get("bigclass.book.output.path", "");
   }
   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException {
	   
	   for (Entry<String, TreeSet<StringIntPair>> entry : classHotBookMap.entrySet()) {
			StringBuffer bookset = new StringBuffer();
			Iterator<StringIntPair> it= entry.getValue().iterator();
			while(it.hasNext()){
				bookset.append(it.next().getFirst()+"|");
			}
			out.write(new Text(entry.getKey().toString()+";"+bookset),NullWritable.get(), classBookOutputPath); 
		 
		} 
		  
		for (Entry<String, TreeSet<StringIntPair>> entry : authorHotBookMap.entrySet()) {
			StringBuffer bookset = new StringBuffer();
			Iterator<StringIntPair> it= entry.getValue().iterator();
			while(it.hasNext()){
				bookset.append(it.next().getFirst()+"|");
			}
			out.write(new Text(entry.getKey().toString()+";"+bookset),NullWritable.get(), authorBookOutputPath); 
		}
		
		for (Entry<String, TreeSet<StringIntPair>> entry : bigclassHotBookMap.entrySet()) {
			StringBuffer bookset = new StringBuffer();
			Iterator<StringIntPair> it= entry.getValue().iterator();
			while(it.hasNext()){
				bookset.append(it.next().getFirst()+"|");
			}
			out.write(new Text(entry.getKey().toString()+";"+bookset),NullWritable.get(), bigclassBookOutputPath); 
		}
 	   out.close();
   }
   
}


