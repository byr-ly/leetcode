package com.eb.bi.rs.mras2.cartoonrec.corelationrec.RealTimeFillerDataPrepare;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
 
public class RealTimeFillerFreeBookPrepareReducer extends Reducer<Text, Text, Text, NullWritable>{
	
  private String authorBookOutputPath;
  private String allBookOutputPath;
  private String bookClickOutputPath;
  private MultipleOutputs<Text, NullWritable> out;
  private HashMap<String,HashSet<String>> authorBook = new HashMap<String,HashSet<String>>();
  private HashSet<String> allBooks = new HashSet<String>();
  private HashMap<String,String> bookClick = new HashMap<String,String>();
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
	  //book_id动漫ID|author_id作者ID|class_id分类ID|charge_type计费类型|点击量
     for(Text value:values){
    	 String[] fields = value.toString().split("\\|", -1);
    	 if(fields.length>=5){
    		 String bookId = fields[0],authorId = fields[1],click = fields[4];
        	 //作家书籍表
        	 if(!authorId.equals("")){
        		 if(authorBook.containsKey(authorId)){
        			 Set<String> books = authorBook.get(authorId);
        			 books.add(bookId);
        		 }else{
        			 HashSet<String> books = new HashSet<String>();
        			 books.add(bookId);
        			 authorBook.put(authorId, books);
        		 }
        	 }
        	 //所有书表
        	 if(!bookId.equals("")){
        		 allBooks.add(bookId);
        	 }
        	 //点击量表
        	 bookClick.put(bookId, click);
    	 }
     }
  }     
  @Override
  protected void setup(Context context) throws IOException, InterruptedException { 
	 out = new MultipleOutputs<Text, NullWritable>(context);
	 Configuration conf = context.getConfiguration();
	 this.authorBookOutputPath = conf.get("author.book.output.path", "");
	 this.allBookOutputPath = conf.get("all.book.output.path", "");
	 this.bookClickOutputPath = conf.get("book.click.output.path","");
  }
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
	   //作者书籍
	   for (Entry<String, HashSet<String>> entry : authorBook.entrySet()) {
			StringBuffer bookset = new StringBuffer();
			Iterator<String> it= entry.getValue().iterator();
			while(it.hasNext()){
				bookset.append(it.next()+"|");
			}
			out.write(new Text(entry.getKey().toString()+";"+bookset),NullWritable.get(), authorBookOutputPath); 
		 
		} 
        //所有书
		StringBuffer bookset = new StringBuffer();
		Iterator<String> it= allBooks.iterator();
		while(it.hasNext()){
			bookset.append(it.next()+"|");
		}
		out.write(new Text("allbook;"+bookset),NullWritable.get(), allBookOutputPath); 
		//点击量表
		for (Entry<String, String> entry : bookClick.entrySet()) {
			out.write(new Text(entry.getKey()+";"+entry.getValue()),NullWritable.get(), bookClickOutputPath); 
		} 
		out.close();
  }
}


